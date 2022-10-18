use std::sync::{Arc, Mutex};

use chrono::Duration;
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

    pub fn get_count(&self) -> u32 {
        *self.count.lock().unwrap()
    }

    pub fn limit_reached(&self) -> bool {
        self.get_count() == self.max_count
    }

    pub fn timeout_occured(&self) -> bool {
        let value = *self.occurred.lock().unwrap();
        if value {
            // reset to see if another timout occurred next time
            *self.occurred.lock().unwrap() = false;
        }
        value
    }

    #[cfg(test)]
    pub fn is_ticking(&self) -> bool {
        self.guard.is_some()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{thread, time::Duration};

    #[test]
    fn timeout() {
        let mut timer = Timer::new(1_i64, 5);
        timer.restart();
        assert!(timer.guard.is_some());
        thread::sleep(Duration::from_secs_f32(2.5_f32));
        timer.pause();
        assert_eq!(timer.get_count(), 2);
        assert!(!timer.limit_reached());
        assert!(timer.timeout_occured())
    }

    #[test]
    fn no_timeout() {
        let mut timer = Timer::new(2_i64, 1);
        timer.restart();
        thread::sleep(Duration::from_secs_f32(1.1_f32));
        timer.pause();
        assert!(!timer.timeout_occured());
        // sleep again but make sure to cross the threshold from the original time out
        thread::sleep(Duration::from_secs_f32(1.5_f32));
        assert!(!timer.timeout_occured());

        assert_eq!(timer.get_count(), 0)
    }

    #[test]
    fn limit() {
        let mut timer = Timer::new(1_i64, 1);
        timer.restart();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.limit_reached());
        // make sure the clamping works right
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.limit_reached());
        assert_eq!(timer.get_count(), 1);
    }

    #[test]
    fn restart_no_fail() {
        let mut timer = Timer::new(2_i64, 3);
        timer.restart();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(!timer.timeout_occured());
        timer.restart();
        thread::sleep(Duration::from_secs_f32(2.2));
        timer.pause();

        assert!(timer.timeout_occured())
    }
}
