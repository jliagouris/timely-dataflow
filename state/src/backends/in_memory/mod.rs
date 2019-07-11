use managed_count::InMemoryManagedCount;
use managed_map::InMemoryManagedMap;
use managed_value::InMemoryManagedValue;

mod managed_count;
mod managed_map;
mod managed_value;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::StateBackend;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

pub struct InMemoryBackend {
    backend: Rc<RefCell<HashMap<String, Rc<Any>>>>,
}

impl StateBackend for InMemoryBackend {
    fn new() -> Self {
        InMemoryBackend {
            backend: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount> {
        Box::new(InMemoryManagedCount::new(name, Rc::clone(&self.backend)))
    }

    fn get_managed_value<V: 'static + FasterValue + FasterRmw>(
        &self,
        name: &str,
    ) -> Box<ManagedValue<V>> {
        Box::new(InMemoryManagedValue::new(name, Rc::clone(&self.backend)))
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue + FasterRmw,
    {
        Box::new(InMemoryManagedMap::new(name, Rc::clone(&self.backend)))
    }
}
