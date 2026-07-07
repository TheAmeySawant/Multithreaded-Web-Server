use std::{
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    usize,
};

/// ## Thread Pool
/// Thread Pool is a pool of **Worker** Threads.
/// Thread Pool assign each job to Worker Threads through **mcpc::channel**.
///
///
/// # Example
/// ```
/// //As long as size is positive, ThreadPool will be created.
/// use multithreaded_web_server::ThreadPool;
/// let pool = ThreadPool::new(8).unwrap();
///
/// pool.execute(|| {
///         //Your code to be runned in parallel on threads goes here.
///     });
/// ```
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

/// ## Job
/// Job type is just a Pointer to FnOnce() closure
type Job = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
    /// Creates a new **ThreadPool**
    pub fn new(size: usize) -> Result<ThreadPool, String> {
        if size == 0 {
            return Err("ThreadPool size can't be zero".to_string());
        }

        let (sender, receiver) = mpsc::channel();
        let mut workers = Vec::with_capacity(size);

        let receiver = Arc::new(Mutex::new(receiver));
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Ok(ThreadPool {
            workers,
            sender: Some(sender),
        })
    }

    /// Creates a Job from closure and then sends it to the mpsc::channel
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender
            .as_ref()
            .expect("[ERROR] Sender doesn't exists or is corrupted")
            .send(job)
            .expect("[ERROR] ThreadPool execute failed to send job through sender!");
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        //Dropping the sender, because we want the 'while let' loop to end in each thread, making each thread end at join(), after completing the in-hand task.
        drop(self.sender.take());

        //vec::drain(range) method removes elements of range from vector and returns an iterator of those elements.
        for worker in &mut self.workers.drain(..) {
            println!("[Dropped] Shutting down worker {}", worker.id);

            //So that ongoing thread compelete their job before shutting down.
            worker.handle.join().expect("[ERROR] worker join failed");
        }
    }
}

/// ## Worker
///
/// Worker keeps a Thread alive.
/// Thread inside Worker always looks for job from receiver and runs the job.
///
pub struct Worker {
    id: usize,
    handle: JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, reciever: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let handle = thread::spawn(move || {
            loop {
                let message = reciever.lock().expect("[ERROR] reciever unlocking failed!").recv();
                match message {
                    Ok(job) => {
                        println!("Worker {id} got a job!");

                        job();
                    }
                    Err(_) => {
                        println!("Worker {id} disconnected; shutting down.");
                        break;
                    }
                }
            }
        });

        Worker { id, handle }
    }

    // fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
    //     let handle = thread::spawn(move || {
    //         while let Ok(job) = {
    //             let rec = receiver.lock().expect("[ERROR] reciever unlocking failed!");

    //             rec.recv()
    //         } {
    //             println!("Worker {id} got a job!");

    //             job();
    //         }
    //         println!("Worker {id} disconnected; shutting down.");
    //     });

    //     Worker { id, handle }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, mpsc as std_mpsc};
    use std::time::Duration;

    #[test]
    fn new_rejects_zero_size() {
        match ThreadPool::new(0) {
            Err(msg) => assert_eq!(msg, "ThreadPool size can't be zero"),
            Ok(_) => panic!("expected an error for size 0"),
        }
    }

    #[test]
    fn new_accepts_positive_size() {
        assert!(ThreadPool::new(1).is_ok());
        assert!(ThreadPool::new(4).is_ok());
    }

    #[test]
    fn execute_runs_a_single_job() {
        let pool = ThreadPool::new(2).unwrap();
        let (tx, rx) = std_mpsc::channel();

        pool.execute(move || {
            tx.send(42).unwrap();
        });

        let result = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn execute_runs_all_submitted_jobs() {
        let pool = ThreadPool::new(4).unwrap();
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..50 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        }

        drop(pool); // blocks until every worker finishes its current job
        assert_eq!(counter.load(Ordering::SeqCst), 50);
    }

    #[test]
    fn jobs_run_concurrently_across_workers() {
        // 4 workers + a barrier of 4: every job must be picked up by a distinct
        // thread at the same time, or the barrier wait deadlocks/times out.
        let size = 4;
        let pool = ThreadPool::new(size).unwrap();
        let barrier = Arc::new(Barrier::new(size));
        let (tx, rx) = std_mpsc::channel();

        for _ in 0..size {
            let barrier = Arc::clone(&barrier);
            let tx = tx.clone();
            pool.execute(move || {
                barrier.wait();
                tx.send(()).unwrap();
            });
        }
        drop(tx);

        for _ in 0..size {
            rx.recv_timeout(Duration::from_secs(2))
                .expect("jobs did not run concurrently in time");
        }
    }

    #[test]
    fn drop_waits_for_in_flight_jobs_to_complete() {
        let flag = Arc::new(AtomicUsize::new(0));
        {
            let pool = ThreadPool::new(1).unwrap();
            let flag = Arc::clone(&flag);
            pool.execute(move || {
                thread::sleep(Duration::from_millis(200));
                flag.store(1, Ordering::SeqCst);
            });
            // pool dropped here; Drop impl must join the worker thread
        }
        assert_eq!(flag.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pool_handles_more_jobs_than_workers() {
        let pool = ThreadPool::new(2).unwrap();
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        }

        drop(pool);
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }
}