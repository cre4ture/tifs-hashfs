pub mod fs_trait;
pub mod trait_impl;

pub use fs_trait::AsyncFileSystem;
pub use trait_impl::AsyncFs;

use std::fmt::Debug;
use std::future::Future;

use tokio::runtime::Handle;
use tokio::task::{block_in_place, spawn};
use tracing::trace;

use super::error::Result;
use super::reply::
    FsReply
;

pub fn spawn_reply<F, R, V>(id: u64, reply: R, f: F)
where
    F: Future<Output = Result<V>> + Send + 'static,
    R: FsReply<V> + Send + 'static,
    V: Debug,
{
    spawn(async move {
        trace!("reply to request({})", id);
        let result = f.await;
        // TODO eprintln!("reply to request({}): {:?}", id, result);
        reply.reply(id, result);
    });
}

fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    block_in_place(move || Handle::current().block_on(future))
}
