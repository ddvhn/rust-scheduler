pub struct Job {
    execution_closure: Box<dyn Fn() -> () + Send + 'static + Sync>,
}

impl Job {
    pub fn new(execution_closure: Box<dyn Fn() -> () + Send + 'static + Sync>) -> Self {
        Self { execution_closure }
    }

    pub fn execute(&self) {
        (self.execution_closure)();
    }
}
