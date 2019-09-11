#[macro_use]
extern crate metrics;
extern crate faster_rs;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use std::hash::Hash;
use std::rc::Rc;

pub mod backends;
pub mod primitives;

pub trait StateBackend: 'static {
    fn new() -> Self;

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount>;
    fn get_managed_value<V: 'static + FasterValue + FasterRmw>(
        &self,
        name: &str,
    ) -> Box<ManagedValue<V>>;
    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue + FasterRmw;
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

    pub fn create_sub_handle(&self, name: &str) -> Self {
        StateHandle {
            backend: Rc::clone(&self.backend),
            name: [&self.name, name].join("."),
        }
    }

    pub fn spawn_new_backend(&self) -> Self {
        StateHandle {
            backend: Rc::new(S::new()),
            name: self.name.clone()
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
        V: 'static + FasterValue + FasterRmw,
    {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_map(&physical_name)
    }

    pub fn get_managed_value<V: 'static + FasterValue + FasterRmw>(
        &self,
        name: &str,
    ) -> Box<ManagedValue<V>> {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_value(&physical_name)
    }
}

impl<S: StateBackend> Clone for StateHandle<S> {
    fn clone(&self) -> Self {
        StateHandle {
            backend: Rc::clone(&self.backend),
            name: self.name.clone(),
        }
    }
}
