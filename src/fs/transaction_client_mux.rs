use std::sync::atomic::AtomicUsize;

use tikv_client::{Config, TransactionClient};



pub struct TransactionClientMux {
    _pd_endpoints: Vec<String>,
    _cfg: Config,
    idx: AtomicUsize,
    clients: Vec<TransactionClient>
}

impl TransactionClientMux {
    pub async fn new(pd_endpoints: Vec<String>, cfg: Config) -> tikv_client::Result<Self> {
        let mut clients = Vec::new();
        for _i in 0..1 {
            clients.push(TransactionClient::new_with_config(pd_endpoints.clone(), cfg.clone()).await?);
        }
        Ok(Self {
            _pd_endpoints: pd_endpoints,
            _cfg: cfg,
            idx: AtomicUsize::new(0),
            clients,
        })
    }

    pub fn give_one(&self) -> &TransactionClient {
        let idx = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.clients[idx % self.clients.len()]
    }
}
