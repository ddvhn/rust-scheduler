use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use rayon::ThreadPoolBuilder;

use crate::job::Job;

pub struct Scheduler {
    job_sender: Sender<Job>,
}

impl Scheduler {
    pub fn new(max_requests_per_sec: u32, num_workers: usize) -> Self {
        let (job_sender, job_receiver) = mpsc::channel::<Job>();
        thread::spawn(move || {
            let thread_pool = ThreadPoolBuilder::new()
                .num_threads(num_workers + 1) // + 1 for pool
                .build()
                .unwrap();
            thread_pool.install(move || {
                rayon::scope(move |s| {
                    let sleep_duration =
                        Duration::from_micros(1_000_000 / max_requests_per_sec as u64);
                    let mut last_received_job_time = Instant::now();
                    println!("did start scope on {:?}", thread::current());
                    loop {
                        if let Ok(job) = job_receiver.recv() {
                            let elapsed = last_received_job_time.elapsed();
                            let wait = sleep_duration
                                .checked_sub(elapsed)
                                .unwrap_or_else(|| Duration::from_secs(0));
                            if wait.as_millis() > 0 {
                                thread::sleep(wait);
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
        Self { job_sender }
    }

    pub fn submit(&self, job: Job) {
        self.job_sender.send(job).unwrap();
    }
}
