use std::{mem, sync::{atomic::AtomicUsize, Arc}};

use tikv_client::{Backoff, Config, Transaction, TransactionClient, TransactionOptions};
use tokio::{sync::RwLock, time::sleep};



pub struct TransactionClientMux {
    _pd_endpoints: Vec<String>,
    _cfg: Config,
    idx: AtomicUsize,
    clients: RwLock<Vec<Option<Arc<TransactionClient>>>>,
    new_client_rx: tokio::sync::watch::Receiver<Arc<TransactionClient>>,
    new_client_tx: tokio::sync::watch::Sender<Arc<TransactionClient>>,
}

impl TransactionClientMux {
    pub async fn new(pd_endpoints: Vec<String>, cfg: Config) -> tikv_client::Result<Self> {
        let mut clients = Vec::new();
        for _i in 0..16 {
            clients.push(Some(Arc::new(TransactionClient::new_with_config(pd_endpoints.clone(), cfg.clone()).await?)));
        }
        let first = (&clients[0]).as_ref().unwrap().clone();
        let (tx, rx) = tokio::sync::watch::channel(first);
        Ok(Self {
            _pd_endpoints: pd_endpoints,
            _cfg: cfg,
            idx: AtomicUsize::new(0),
            clients: RwLock::new(clients),
            new_client_rx: rx,
            new_client_tx: tx,
        })
    }

    pub async fn give_one_transaction(&self, options: &TransactionOptions) -> tikv_client::Result<Transaction> {
        loop {
            let (client, id) = self.give_one().await?;
            let result = client.begin_with_options(options.clone()).await;
            if result.is_err() {
                self.replace_one_due_to_error(id).await?;
            } else {
                return Ok(result.unwrap());
            }
        }
    }

    pub async fn give_one(&self) -> tikv_client::Result<(Arc<TransactionClient>, usize)> {
        let mut i = 0;
        loop {
            let idx = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let lock = self.clients.read().await;
            let id = idx % lock.len();
            let client = &lock[id];
            if let Some(client) = client {
                return Ok((client.clone(), id));
            }
            i += 1;
            if i > lock.len() {
                let mut channel = self.new_client_rx.clone();
                channel.changed().await.map_err(|err|{
                    tikv_client::Error::InternalError { message: format!("channel was closed. Err: {err:?}") }
                })?;
                i = 0;
            }
        }
    }

    pub async fn replace_one_due_to_error(&self, id: usize) -> tikv_client::Result<()> {
        let mut lock = self.clients.write().await;
        if id >= lock.len() {
            return Ok(());
        }
        let mut prev = mem::take(lock.get_mut(id).unwrap());
        drop(lock);

        let we_were_first = prev.take().is_some();
        if we_were_first {
            let mut backoff = Backoff::decorrelated_jitter_backoff(100, 1000, 100);
            let client = loop {
                let new_client = self.make_one().await;
                match new_client {
                    Ok(client) => break client,
                    Err(err) => {
                        if let Some(delay) = backoff.next_delay_duration() {
                            sleep(delay).await;
                        } else {
                            return Err(err);
                        }
                    }
                }
            };
            let arc = Arc::new(client);
            let mut lock = self.clients.write().await;
            lock[id] = Some(arc.clone());
            drop(lock);
            self.new_client_tx.send(arc).map_err(|err|{
                tikv_client::Error::InternalError { message: format!("channel was closed. Err: {err:?}") }
            })?;
        }
        Ok(())
    }

    async fn make_one(&self) -> tikv_client::Result<TransactionClient> {
        TransactionClient::new_with_config(self._pd_endpoints.clone(), self._cfg.clone()).await
    }
}
