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
                    println!("did start scope on {:?}", thread::current());
                    let mut last_received_job_time = Instant::now();
                    loop {
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
        Self { job_sender }
    }

    pub fn submit(&self, job: Job) {
        self.job_sender.send(job).unwrap();
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
