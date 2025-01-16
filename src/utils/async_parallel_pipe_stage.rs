use std::{collections::VecDeque, mem, sync, sync::Arc};

use futures::{stream::FuturesUnordered, Future, StreamExt};
use tokio::task::JoinHandle;


pub struct AsyncParallelPipeStage<F: Future + Send> {
    in_progress_limit: usize,
    total: sync::Mutex<usize>,
    in_progress: tokio::sync::Mutex<FuturesUnordered<JoinHandle<()>>>,
    results: Arc<sync::Mutex<VecDeque<<F as Future>::Output>>>,
}

impl<F: Future + Send> AsyncParallelPipeStage<F>
where
    F: 'static,
    F::Output: Send + 'static,
{
    pub fn new(mut in_progress_limit: usize) -> Self {
        if in_progress_limit == 0 {
            in_progress_limit = 1;
        }
        Self {
            in_progress_limit,
            total: sync::Mutex::new(0),
            in_progress: tokio::sync::Mutex::new(FuturesUnordered::new()),
            results: Arc::new(sync::Mutex::new(VecDeque::new())),
        }
    }

    pub async fn push(&self, future: F) {

        let lock;
        loop {
            let mut queue = self.in_progress.lock().await;
            let current_in_progress = queue.len();
            if current_in_progress < self.in_progress_limit {
                lock = queue;
                break;
            }
            queue.next().await.unwrap().unwrap();
            mem::drop(queue);
        }

        *self.total.lock().unwrap() += 1;
        let results_collection = self.results.clone();
        let reporting_fut = async move {
            let r = future.await;
            results_collection.lock().unwrap().push_back(r);
        };
        lock.push(tokio::spawn(reporting_fut));
    }

    pub async fn wait_finish_all(&self) {
        let mut lock = self.in_progress.lock().await;
        let remaining = mem::take(&mut *lock);
        std::mem::drop(lock);

        for fut in remaining.into_iter() {
            fut.await.unwrap();
        }
    }

    pub fn get_total(&self) -> usize {
        *self.total.lock().unwrap()
    }

    pub async fn get_results_so_far(&self) -> VecDeque<<F as Future>::Output> {
        let mut lock = self.results.lock().unwrap();
        mem::take(&mut lock)
    }
}

#[cfg(test)]
mod tests {
    use std::{future::IntoFuture, sync::Arc, thread::sleep, time::Duration};

    use futures::{channel::oneshot::Sender, future::BoxFuture, FutureExt};

    use super::*;

    #[tokio::test]
    async fn test_async_parallel_pipe_stage() {
        let stage: AsyncParallelPipeStage<BoxFuture<'static, usize>> = AsyncParallelPipeStage::new(2);

        let f1 = async { 1 }.boxed();
        let f2 = async { 2 }.boxed();
        let f3 = async { 3 }.boxed();
        let f4 = async { 4 }.boxed();

        stage.push(f1).await;
        stage.push(f2).await;
        stage.push(f3).await;
        stage.push(f4).await;

        stage.wait_finish_all().await;

        let results = stage.get_results_so_far().await;
        assert_eq!(results, vec![1, 2, 3, 4]);
        assert_eq!(stage.get_total(), 4);
    }

    pub fn test_thread_signal<T: 'static + Send>() -> (Sender<T>, std::pin::Pin<Box<dyn Future<Output = T> + Send>>) {
        let (s1, r1) = futures::channel::oneshot::channel();
        let f1: std::pin::Pin<Box<dyn Future<Output = T> + Send>> = r1.into_future().map(|f|f.unwrap()).boxed();
        (s1, f1)
    }

    #[tokio::test]
    async fn test_async_parallel_pipe_stage2_0() {
        let stage: AsyncParallelPipeStage<BoxFuture<'static, usize>> = AsyncParallelPipeStage::new(0);

        let (s1, f1) = test_thread_signal();
        let (s2, f2) = test_thread_signal();
        let (s3, f3) = test_thread_signal();
        let (s4, f4) = test_thread_signal();

        s1.send(1001).unwrap();
        s2.send(1002).unwrap();
        s3.send(1003).unwrap();
        s4.send(1004).unwrap();

        stage.push(f1).await;
        stage.push(f2).await;
        stage.push(f3).await;
        stage.push(f4).await;

        stage.wait_finish_all().await;

        let results = stage.get_results_so_far().await;
        assert_eq!(results, vec![1001, 1002, 1003, 1004]);
        assert_eq!(stage.get_total(), 4);
    }

    pub fn spawn_stage_push(
        stage: Arc<AsyncParallelPipeStage<std::pin::Pin<Box<dyn Future<Output = i32> + Send>>>>,
        f: std::pin::Pin<Box<dyn Future<Output = i32> + Send>>,
    ) -> tokio::task::JoinHandle<()> {
        let p1 = tokio::spawn(async move {
            stage.push(f).await
        });
        p1
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_async_parallel_pipe_stage2_2() {
        let stage: Arc<AsyncParallelPipeStage<std::pin::Pin<Box<dyn Future<Output = i32> + Send>>>> = Arc::new(AsyncParallelPipeStage::new(2));

        print!("created stage...");

        let (s1, f1) = test_thread_signal();
        let (s2, f2) = test_thread_signal();
        let (s3, f3) = test_thread_signal();
        let (s4, f4) = test_thread_signal();


        let p1 = spawn_stage_push(stage.clone(), f1);
        let p2 = spawn_stage_push(stage.clone(), f2);
        let p3 = spawn_stage_push(stage.clone(), f3);
        let p4 = spawn_stage_push(stage.clone(), f4);

        sleep(Duration::from_millis(100));

        assert_eq!(stage.clone().get_results_so_far().await.len(), 0);

        s1.send(1001).unwrap();
        s2.send(1002).unwrap();
        s3.send(1003).unwrap();
        s4.send(1004).unwrap();

        p1.await.unwrap();
        p2.await.unwrap();
        p3.await.unwrap();
        p4.await.unwrap();

        stage.clone().wait_finish_all().await;

        let results = stage.clone().get_results_so_far().await;
        assert_eq!(results.len(), 4);
        assert!(results.contains(&1001));
        assert!(results.contains(&1002));
        assert!(results.contains(&1003));
        assert!(results.contains(&1004));
        assert_eq!(stage.clone().get_total(), 4);

    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_async_parallel_pipe_stage2_3() {
        let stage: Arc<AsyncParallelPipeStage<std::pin::Pin<Box<dyn Future<Output = i32> + Send>>>> = Arc::new(AsyncParallelPipeStage::new(2));

        print!("created stage...");

        let (s1, f1) = test_thread_signal();
        let (s2, f2) = test_thread_signal();
        let (s3, f3) = test_thread_signal();
        let (s4, f4) = test_thread_signal();


        let p1 = spawn_stage_push(stage.clone(), f1);
        let p2 = spawn_stage_push(stage.clone(), f2);
        let p3 = spawn_stage_push(stage.clone(), f3);
        let p4 = spawn_stage_push(stage.clone(), f4);

        sleep(Duration::from_millis(100));

        assert_eq!(stage.clone().get_results_so_far().await.len(), 0);

        s1.send(1001).unwrap();
        s2.send(1002).unwrap();

        p1.await.unwrap();
        p2.await.unwrap();

        sleep(Duration::from_millis(100));

        {
            let results = stage.clone().get_results_so_far().await;
            assert!(results.contains(&1001));
            assert!(results.contains(&1002));
            assert_eq!(results.len(), 2);
        }

        s3.send(1003).unwrap();
        s4.send(1004).unwrap();

        p3.await.unwrap();
        p4.await.unwrap();

        stage.clone().wait_finish_all().await;

        let results = stage.clone().get_results_so_far().await;
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1003));
        assert!(results.contains(&1004));
        assert_eq!(stage.clone().get_total(), 4);

    }

}
