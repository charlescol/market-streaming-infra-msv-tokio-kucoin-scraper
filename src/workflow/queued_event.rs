pub struct QueuedEvent<T> {
    pub enq: std::time::Instant,
    pub deq: Option<std::time::Instant>,
    pub msg: T,
}

impl<T> QueuedEvent<T> {
    pub fn new(msg: T) -> Self {
        Self {
            enq: std::time::Instant::now(),
            msg,
            deq: None,
        }
    }

    pub fn set_deq(&mut self) {
        self.deq = Some(std::time::Instant::now());
    }
}
