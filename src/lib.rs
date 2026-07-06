use std::{
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    usize,
};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
}
type Job = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
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

        Ok(ThreadPool { workers, sender })
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender
            .send(job)
            .expect("ThreadPool execute failed to send job through sender!");
    }
}

pub struct Worker {
    id: usize,
    handle: JoinHandle<()>,
    // reciever: Arc<Mutex<mpsc::Receiver<Job>>>
}

impl Worker {
    fn new(id: usize, reciever: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let handle = thread::spawn(move || {
            loop {
                let job = reciever
                    .lock()
                    .expect("reciever unlocking failed!")
                    .recv()
                    .expect("recieving job failed!");

                println!("Woker {id} got a job!");

                job();
            }
        });

        Worker { id, handle }
    }
}
