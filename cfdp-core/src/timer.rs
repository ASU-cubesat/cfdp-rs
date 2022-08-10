use std::sync::{Arc, Mutex};

use time::Duration;
use timer::{Guard, Timer as Counter};

pub struct Timer {
    timer: Counter,
    guard: Option<Guard>,
    timeout: Duration,
    max_count: u32,
    count: Arc<Mutex<u32>>,
    occurred: Arc<Mutex<bool>>,
}
impl Timer {
    pub fn new(timeout: i64, max_count: u32) -> Self {
        Self {
            timer: Counter::new(),
            guard: None,
            max_count,
            timeout: Duration::seconds(timeout),
            count: Arc::new(Mutex::new(0_u32)),
            occurred: Arc::new(Mutex::new(false)),
        }
    }

    pub fn restart(&mut self) {
        if self.guard.is_some() {
            self.guard = None;
        }

        // reset timeout flag
        *self.occurred.lock().unwrap() = false;

        self.guard = {
            let count = self.count.clone();
            let max = self.max_count;
            let occurred = self.occurred.clone();
            Some(self.timer.schedule_repeating(self.timeout, move || {
                *occurred.lock().unwrap() = true;

                let mut c = count.lock().unwrap();
                *c = (*c + 1).clamp(0, max)
            }))
        };
    }

    pub fn pause(&mut self) {
        if self.guard.is_some() {
            self.guard = None
        }
    }

    pub fn limit_reached(&self) -> bool {
        *self.count.lock().unwrap() == self.max_count
    }

    pub fn timout_occured(&self) -> bool {
        *self.occurred.lock().unwrap()
    }
}
