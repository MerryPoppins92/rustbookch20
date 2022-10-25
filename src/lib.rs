use std::{sync::{mpsc, Arc, Mutex}, thread};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>
}

// pub struct Job;

pub struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>
    
}

impl Worker {
    pub fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let thread = thread::spawn(move || loop {
            match receiver.lock().unwrap().recv() {
                Ok(job) => {
                    println!("worker {id} got a job; execution");
                    job();
                }
                Err(_) => {
                    println!("worker {id} disconnected; shutting down");
                    break;
                }
            }
        });
        Worker { id, thread: Some(thread) }
    }
}

#[derive(Debug)]
pub enum PoolCreationError {
    Zero,
    Negatif,
}

type Job = Box<dyn FnOnce() + Send + 'static>;


impl ThreadPool {
    pub fn build(size: i32) -> Result<ThreadPool, PoolCreationError> {
        if size < 0 {
            return Err(PoolCreationError::Negatif);
        } else if size == 0 {
            return Err(PoolCreationError::Zero);
        } else {
            let (sender, receiver) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            let mut workers = Vec::with_capacity(size as usize);
            for id in 0..size {
                workers.push(Worker::new(id as usize, Arc::clone(&receiver)));
            }
            Ok(ThreadPool { workers, sender: Some(sender) })
        }
        
    }
    pub fn execute<F>(&self, f: F) 
    where 
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.sender.as_ref().unwrap().send(job).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.workers {
            println!("shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take(){
                thread.join().unwrap();
            }
        }
    }
    
}