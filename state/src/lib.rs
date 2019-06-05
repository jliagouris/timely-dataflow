extern crate faster_rs;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use faster_rs::{FasterKey, FasterKv, FasterValue};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

pub mod backends;
pub mod primitives;

#[derive(Clone)]
pub struct StateBackendInfo {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

impl StateBackendInfo {
    pub fn new(faster: &Rc<FasterKv>) -> Self {
        StateBackendInfo {
            faster: Rc::clone(faster),
            monotonic_serial_number: Rc::new(RefCell::new(0)),
        }
    }
}

pub trait StateBackend: 'static {
    fn new(info: &StateBackendInfo) -> Self;

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount>;
    fn get_managed_value<V: 'static + FasterValue>(&self, name: &str) -> Box<ManagedValue<V>>;
    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue;
}

pub struct StateHandle<S: StateBackend> {
    backend: Rc<S>,
    name: String,
}

impl<S: StateBackend> StateHandle<S> {
    pub fn new(backend: Rc<S>, name: &str) -> Self {
        StateHandle {
            backend,
            name: name.to_owned(),
        }
    }

    pub fn get_managed_count(&self, name: &str) -> Box<ManagedCount> {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_count(&physical_name)
    }

    pub fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue,
    {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_map(&physical_name)
    }

    pub fn get_managed_value<V: 'static + FasterValue>(&self, name: &str) -> Box<ManagedValue<V>> {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_value(&physical_name)
    }
}
