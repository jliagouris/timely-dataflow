use crate::{StateBackend, StateBackendInfo};

use faster_rs::{status, FasterKv, FasterValue};
use std::cell::RefCell;
use std::rc::Rc;

pub struct FASTERBackend {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

impl FASTERBackend {
    fn maybe_refresh(&self, monotonic_serial_number: u64) {
        if monotonic_serial_number % 64 == 0 {
            self.faster.refresh();
            if monotonic_serial_number % 1600 == 0 {
                self.faster.complete_pending(false);
            }
        }
    }
}

impl StateBackend for FASTERBackend {
    fn new(info: &StateBackendInfo) -> Self {
        FASTERBackend {
            faster: Rc::clone(&info.faster),
            monotonic_serial_number: Rc::clone(&info.monotonic_serial_number),
        }
    }

    fn store_count(&mut self, name: &str, count: u64) {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        self.faster
            .upsert(&name, &count, old_monotonic_serial_number);
        self.maybe_refresh(old_monotonic_serial_number);
    }

    fn get_count(&self, name: &str) -> u64 {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        let (status, recv) = self.faster.read(&name, old_monotonic_serial_number);
        if status != status::OK {
            return 0;
        }
        return match recv.recv() {
            Ok(count) => count,
            Err(_) => 0,
        };
    }

    fn store_value<T: 'static + FasterValue>(&mut self, name: &str, value: T) {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        self.faster
            .upsert(&name, &value, old_monotonic_serial_number);
    }

    fn get_value<T: 'static + FasterValue>(&mut self, name: &str) -> Option<T> {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        let (status, recv) = self.faster.read(&name, old_monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        };
    }
}
