use std::{collections::VecDeque, mem};

use futures::{stream::FuturesUnordered, Future, StreamExt};
use tokio::task::JoinHandle;


pub struct AsyncParallelPipeStage<F: Future + Send> {
    pub in_progress_limit: usize,
    pub in_progress: FuturesUnordered<JoinHandle<F::Output>>,
    pub results: VecDeque<<F as Future>::Output>,
    total: usize,
}

impl<F: Future + Send> AsyncParallelPipeStage<F>
where
    F: 'static,
    F::Output: Send + 'static,
{
    pub fn new(in_progress_limit: usize) -> Self {
        Self {
            in_progress_limit,
            in_progress: FuturesUnordered::new(),
            results: VecDeque::new(),
            total: 0
        }
    }

    pub async fn push(&mut self, future: F) {
        self.total += 1;
        self.in_progress.push(tokio::spawn(future));

        if self.in_progress.len() >= self.in_progress_limit {
            let done = self.in_progress.next().await.unwrap().unwrap();
            self.results.push_back(done);
        }
    }

    pub async fn wait_finish_all(&mut self) {
        for fut in mem::take(&mut self.in_progress).into_iter() {
            let done = fut.await.unwrap();
            self.results.push_back(done);
        }
    }

    pub fn get_total(&self) -> usize {
        self.total
    }

    pub fn get_results_so_far(&mut self) -> VecDeque<<F as Future>::Output> {
        mem::take(&mut self.results)
    }
}
