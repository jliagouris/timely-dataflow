extern crate faster_rs;

use crate::primitives::{ManagedCount, ManagedValue};
use faster_rs::{FasterKv, FasterValue};
use std::cell::RefCell;
use std::rc::Rc;

pub mod backends;
mod primitives;

#[derive(Clone)]
pub struct StateBackendInfo {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

impl StateBackendInfo {
    pub fn new(faster: FasterKv) -> Self {
        StateBackendInfo {
            faster: Rc::new(faster),
            monotonic_serial_number: Rc::new(RefCell::new(1)),
        }
    }
}

pub trait StateBackend: 'static {
    fn new(info: &StateBackendInfo) -> Self;

    fn store_count(&mut self, name: &str, count: u64);
    fn get_count(&self, name: &str) -> u64;

    fn store_value<T: 'static + FasterValue>(&mut self, name: &str, value: T);
    fn get_value<T: 'static + FasterValue>(&mut self, name: &str) -> Option<T>;
}

pub struct StateHandle<S: StateBackend> {
    backend: Rc<RefCell<S>>,
    name: String,
}

impl<S: StateBackend> StateHandle<S> {
    pub fn new(backend: Rc<RefCell<S>>, name: &str) -> Self {
        StateHandle {
            backend,
            name: name.to_owned(),
        }
    }

    pub fn get_managed_count(&self, name: &str) -> ManagedCount<S> {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        ManagedCount::new(self.backend.clone(), &physical_name)
    }

    pub fn get_managed_value(&self, name: &str) -> ManagedValue<S> {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        ManagedValue::new(self.backend.clone(), &physical_name)
    }
}
