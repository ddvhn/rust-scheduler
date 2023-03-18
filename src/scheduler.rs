use std::sync::mpsc::Sender;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use rayon::ThreadPoolBuilder;

use crate::job::Job;

pub struct Scheduler {
    job_sender: Arc<Mutex<Sender<Job>>>,
    is_terminated: Arc<AtomicBool>,
    worker_handle: Option<thread::JoinHandle<()>>,
}

impl Scheduler {
    pub fn new(max_requests_per_sec: u32, num_workers: usize) -> Self {
        let (job_sender, job_receiver) = mpsc::channel::<Job>();
        let is_terminated = Arc::new(AtomicBool::new(false));
        let is_terminated_clone = is_terminated.clone();
        let worker_handle = thread::spawn(move || {
            let thread_pool = ThreadPoolBuilder::new()
                .num_threads(num_workers + 1) // + 1 for pool
                .build()
                .unwrap();
            thread_pool.install(move || {
                rayon::scope(move |s| {
                    let mut last_received_job_time = Instant::now();
                    loop {
                        if is_terminated_clone.load(Ordering::SeqCst) {
                            break;
                        }
                        if let Ok(job) = job_receiver.recv() {
                            let sleep_time = Self::calculate_sleep_time(
                                last_received_job_time,
                                max_requests_per_sec as f32,
                            );
                            if sleep_time.as_millis() > 0 {
                                thread::sleep(sleep_time);
                            }
                            last_received_job_time = Instant::now();
                            s.spawn(move |_| {
                                job.execute();
                            });
                        }
                    }
                });
            });
        });
        let job_sender = Arc::new(Mutex::new(job_sender));
        Self {
            job_sender,
            is_terminated,
            worker_handle: Some(worker_handle),
        }
    }

    pub fn submit(&self, job: Job) {
        let job_sender = self.job_sender.lock().unwrap();
        job_sender.send(job).unwrap();
    }

    pub fn terminate(&mut self) {
        self.is_terminated.store(true, Ordering::SeqCst);
        if let Some(handle) = self.worker_handle.take() {
            handle.join().unwrap();
        }
    }

    fn calculate_sleep_time(last_update_time: Instant, refill_rate: f32) -> Duration {
        let elapsed_time = last_update_time.elapsed().as_secs_f32();

        let leak_amount = elapsed_time * refill_rate; // amount leaked from the bucket since last update
        let bucket_level = 1.0 - leak_amount; // current level of the bucket

        if bucket_level <= 0.0 {
            Duration::from_secs(0) // bucket has been emptied completely, no need to sleep
        } else {
            let refill_time = bucket_level / refill_rate; // time required to refill the bucket to 100%
            Duration::from_secs_f32(refill_time) // sleep duration required to refill the bucket to 100%
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.terminate();
    }
}
