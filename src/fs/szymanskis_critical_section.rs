
use std::sync::Arc;

use strum::{EnumIter, IntoEnumIterator};
use tikv_client::{BoundRange, Key, KvPair};
use uuid::Uuid;

use super::{error::TiFsResult, flexible_transaction::FlexibleTransaction, key::ScopedKeyBuilder, transaction::MAX_TIKV_SCAN_LIMIT};

type MyTxn = FlexibleTransaction;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy, EnumIter)]
pub enum SzymanskisState {
    S0Noncritical,
    S1Intention,
    S2WaitingRoom,
    S3DoorIn,
    S4DoorOut,
}

impl SzymanskisState {
    pub fn as_u8(&self) -> u8 {
        let iter = SzymanskisState::iter();
        let id_u8 = iter.enumerate().fold(
            0 as u8, |acc,(i,s)| {
                if s == *self {
                    i as u8
                } else {
                    acc
                }
        });
        id_u8
    }

    pub fn from_u8(id_u8: u8) -> SzymanskisState {
        let iter = SzymanskisState::iter();
        let state = iter.enumerate().fold(
            SzymanskisState::S0Noncritical, |acc,(i,s)| {
                if i == id_u8 as usize {
                    s
                } else {
                    acc
                }
        });
        state
    }
}

pub struct SzymanowskiCriticalSection {
    key_to_lock: Vec<u8>,
    my_id: Uuid,
    my_state: SzymanskisState,
    key_for_state_report: Vec<u8>,
    key_range: BoundRange,
    entered: bool,
}

impl SzymanowskiCriticalSection {
    pub fn new(key_prefix: Vec<u8>, key_to_lock: Vec<u8>) -> Self {
        let my_id = uuid::Uuid::new_v4();
        let key_for_state_report = ScopedKeyBuilder::new(&key_prefix)
            .key_lock_state(&key_to_lock, my_id);
        let key_range_start = ScopedKeyBuilder::new(&key_prefix)
            .key_lock_state(&key_to_lock, Uuid::nil());
        let key_range_end = ScopedKeyBuilder::new(&key_prefix)
            .key_lock_state(&key_to_lock, Uuid::max());
        Self {
            key_to_lock,
            my_id,
            my_state: SzymanskisState::S0Noncritical,
            key_for_state_report,
            key_range: (Key::from(key_range_start)..=Key::from(key_range_end)).into(),
            entered: false,
        }
    }

    pub fn entered(&self) -> bool {
        self.entered
    }

    pub async fn enter(&mut self, txn: &FlexibleTransaction) -> TiFsResult<()> {

        if self.entered {
            return TiFsResult::Ok(())
        }

        use SzymanskisState::*;
        self.change_own_state(S1Intention, txn).await?;
        self.poll_for_condition(txn, |states|{
            states.iter().fold(true, |acc, (_, v)|{
                acc && (S0Noncritical..=S2WaitingRoom).contains(v)
            })
        }).await?;
        self.change_own_state(S3DoorIn, txn).await?;
        let another_waiting = self.get_current_states_excluding_mine(txn).await?.into_iter().fold(
            false, |acc, (_, v)|{
                acc || (v == S1Intention)
            });
        if another_waiting {
            self.change_own_state(S2WaitingRoom, txn).await?;
            self.poll_for_condition(txn, |states|{
                states.iter().fold(false, |acc, (_, v)|{
                    acc || (*v == S4DoorOut)
                })
            }).await?;
        }
        let my_id = self.my_id;
        self.change_own_state(S4DoorOut, txn).await?;
        self.poll_for_condition(txn, |states|{
            states.iter().fold(true, |acc, (id, v)|{
                acc && (
                    (id >= &my_id) ||
                    (S0Noncritical..=S1Intention).contains(v)
                )
            })
        }).await?;
        self.entered = true;
        Ok(())
    }

    pub async fn leave_arc(mut self, txn: Arc<FlexibleTransaction>) -> TiFsResult<()> {
        self.leave(&txn).await
    }

    pub async fn leave(&mut self, txn: &FlexibleTransaction) -> TiFsResult<()> {

        if !self.entered {
            return Ok(())
        }

        use SzymanskisState::*;
        let my_id = self.my_id;
        self.poll_for_condition(txn, |states|{
            states.iter().fold(true, |acc, (id, v)|{
                acc && (
                    (id <= &my_id) ||
                    [S0Noncritical, S1Intention, S4DoorOut].contains(v)
                )
            })
        }).await?;
        self.change_own_state(S0Noncritical, txn).await?;
        self.entered = false;
        Ok(())
    }

    async fn change_own_state(&mut self, new_state: SzymanskisState, txn: &MyTxn
    ) -> TiFsResult<()> {
        if new_state == SzymanskisState::S0Noncritical {
            txn.delete(self.key_for_state_report.clone()).await?;
        } else {
            let new_state_id = new_state.as_u8();
            txn.put(self.key_for_state_report.clone(),
                vec![new_state_id]).await?;
        }
        self.my_state = new_state;
        Ok(())
    }

    async fn poll_for_condition(&mut self, txn: &FlexibleTransaction, predicate: impl Fn(&[(Uuid, SzymanskisState)]) -> bool) -> TiFsResult<()> {
        loop {
            let all_states = self.get_current_states_excluding_mine(txn).await?;
            let done = predicate(&all_states);
            if done {
                return Ok(());
            }
        }
    }

    async fn get_current_states_excluding_mine(&mut self, txn: &FlexibleTransaction) -> TiFsResult<Vec<(Uuid, SzymanskisState)>> {
        let all_states_kv = txn.scan(self.key_range.clone(), MAX_TIKV_SCAN_LIMIT).await?;
        let all_states = all_states_kv.into_iter().filter_map(
            |KvPair(k,v)| {
                let key_buffer = Vec::from(k);
                let i = key_buffer.into_iter();
                let TiFsResult::Ok(parsed_uuid) = txn.fs_config().key_parser(i).and_then(|kp|{
                    kp.parse_lock_key(&self.key_to_lock)
                 }) else {
                    tracing::error!("parsing of lock state key failed!");
                    return None;
                };
                (parsed_uuid != self.my_id).then(||{
                    (parsed_uuid, SzymanskisState::from_u8(
                        *v.get(0).unwrap_or(&0u8)))
                })
            }).collect::<Vec<_>>();
        Ok(all_states)
    }
}


pub struct CriticalSectionKeyLock {
    critical_section: Option<SzymanowskiCriticalSection>,
    txn: Arc<FlexibleTransaction>,
}

impl CriticalSectionKeyLock {
    pub async fn new(txn: Arc<FlexibleTransaction>, key_to_lock: Vec<u8>) -> TiFsResult<Self> {
        let mut cs = SzymanowskiCriticalSection::new(
            txn.fs_config().key_prefix.clone(), key_to_lock);
        cs.enter(&txn).await?;
        TiFsResult::Ok(Self {
            critical_section: Some(cs),
            txn,
        })
    }

    /// Call to this function is optional as the Drop trait is implemented.
    /// Still it makes sense to call it explicitly, as error reporting and
    /// wait for completion can not be done with Drop trait.
    pub async fn unlock(mut self) -> TiFsResult<()> {
        if let Some(mut cs) = self.critical_section.take() {
            cs.leave(&self.txn).await
        } else {
            Ok(())
        }
    }
}

impl Drop for CriticalSectionKeyLock {
    fn drop(&mut self) {
        let cs = self.critical_section.take();
        let txn = self.txn.clone();
        if let Some(cs) = cs {
            tokio::spawn((move || {
                cs.leave_arc(txn)
            })());
        }
    }
}
