use std::thread;

pub struct Job {
    job_id: u32,
    execution_closure: Box<dyn Fn() -> () + Send + 'static + Sync>,
}

impl Job {
    pub fn new(
        job_id: u32,
        execution_closure: Box<dyn Fn() -> () + Send + 'static + Sync>,
    ) -> Self {
        Self {
            job_id,
            execution_closure,
        }
    }

    pub fn execute(&self) {
        let current_thread = thread::current();
        println!("executing job #{} on {:?}", self.job_id, current_thread);
        (self.execution_closure)();
    }
}
