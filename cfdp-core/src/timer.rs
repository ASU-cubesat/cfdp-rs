use std::sync::{Arc, Mutex};

use chrono::Duration;
use timer::{Guard, Timer as ThreadTimer};

pub struct Counter {
    guard: Option<Guard>,
    timeout: Duration,
    max_count: u32,
    count: Arc<Mutex<u32>>,
    occurred: Arc<Mutex<bool>>,
}
impl Counter {
    fn new(timeout: i64, max_count: u32) -> Self {
        Self {
            guard: None,
            max_count,
            timeout: Duration::seconds(timeout),
            count: Arc::new(Mutex::new(0_u32)),
            occurred: Arc::new(Mutex::new(false)),
        }
    }
    fn restart(&mut self, timer: &mut ThreadTimer) {
        if self.guard.is_some() {
            self.guard = None;
        }

        // reset timeout flag
        *self.occurred.lock().unwrap() = false;

        self.guard = {
            let count = self.count.clone();
            let max = self.max_count;
            let occurred = self.occurred.clone();
            Some(timer.schedule_repeating(self.timeout, move || {
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

pub struct Timer {
    timer: ThreadTimer,
    pub inactivity: Counter,
    pub ack: Counter,
    pub nak: Counter,
}
impl Timer {
    pub fn new(
        inactivity_timeout: i64,
        inactivity_max_count: u32,
        ack_timeout: i64,
        ack_max_count: u32,
        nak_timeout: i64,
        nak_max_count: u32,
    ) -> Self {
        Self {
            timer: ThreadTimer::new(),
            inactivity: Counter::new(inactivity_timeout, inactivity_max_count),
            ack: Counter::new(ack_timeout, ack_max_count),
            nak: Counter::new(nak_timeout, nak_max_count),
        }
    }

    pub fn restart_inactivity(&mut self) {
        self.inactivity.restart(&mut self.timer)
    }

    pub fn restart_ack(&mut self) {
        self.ack.restart(&mut self.timer)
    }

    pub fn restart_nak(&mut self) {
        self.nak.restart(&mut self.timer)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{thread, time::Duration};

    #[test]
    fn timeout() {
        let mut timer = Timer::new(1_i64, 5, 1_i64, 5, 1_i64, 5);
        timer.restart_inactivity();
        assert!(timer.inactivity.guard.is_some());
        thread::sleep(Duration::from_secs_f32(2.5_f32));
        timer.inactivity.pause();
        assert_eq!(timer.inactivity.get_count(), 2);
        assert!(!timer.inactivity.limit_reached());
        assert!(timer.inactivity.timeout_occured())
    }

    #[test]
    fn no_timeout() {
        let mut timer = Timer::new(3_i64, 5, 1_i64, 5, 1_i64, 5);
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(1.1_f32));
        timer.inactivity.pause();
        assert!(!timer.inactivity.timeout_occured());
        // sleep again but make sure to cross the threshold from the original time out
        thread::sleep(Duration::from_secs_f32(1.5_f32));
        assert!(!timer.inactivity.timeout_occured());

        assert_eq!(timer.inactivity.get_count(), 0)
    }

    #[test]
    fn limit() {
        let mut timer = Timer::new(1_i64, 1, 1_i64, 5, 1_i64, 5);
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.inactivity.limit_reached());
        // make sure the clamping works right
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.inactivity.limit_reached());
        assert_eq!(timer.inactivity.get_count(), 1);
    }

    #[test]
    fn restart_no_fail() {
        let mut timer = Timer::new(2_i64, 5, 1_i64, 5, 1_i64, 5);
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(!timer.inactivity.timeout_occured());
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(2.2));
        timer.inactivity.pause();

        assert!(timer.inactivity.timeout_occured())
    }

    #[test]
    fn timeout_all() {
        let mut timer = Timer::new(1_i64, 5, 1_i64, 5, 1_i64, 5);
        timer.restart_inactivity();
        timer.restart_ack();
        timer.restart_nak();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.inactivity.timeout_occured());
        assert!(timer.ack.timeout_occured());
        assert!(timer.nak.timeout_occured());
        assert_eq!(timer.inactivity.get_count(), 1);
        assert_eq!(timer.ack.get_count(), 1);
        assert_eq!(timer.nak.get_count(), 1);
    }
}
