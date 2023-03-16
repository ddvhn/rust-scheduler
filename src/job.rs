use std::thread;

pub struct Job {
    execution_closure: Box<dyn Fn() -> () + Send + 'static + Sync>,
}

impl Job {
    pub fn new(execution_closure: Box<dyn Fn() -> () + Send + 'static + Sync>) -> Self {
        Self { execution_closure }
    }

    pub fn execute(&self) {
        let current_thread = thread::current();
        (self.execution_closure)();
    }
}
