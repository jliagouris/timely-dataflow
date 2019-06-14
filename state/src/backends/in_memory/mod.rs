use managed_count::InMemoryManagedCount;
use managed_map::InMemoryManagedMap;
use managed_value::InMemoryManagedValue;

mod managed_count;
mod managed_map;
mod managed_value;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::StateBackend;
use faster_rs::{FasterKey, FasterValue};
use std::hash::Hash;

pub struct InMemoryBackend {}

impl StateBackend for InMemoryBackend {
    fn new() -> Self {
        InMemoryBackend {}
    }

    fn get_managed_count(&self, _name: &str) -> Box<ManagedCount> {
        Box::new(InMemoryManagedCount::new())
    }

    fn get_managed_value<V: 'static + FasterValue>(&self, _name: &str) -> Box<ManagedValue<V>> {
        Box::new(InMemoryManagedValue::new())
    }

    fn get_managed_map<K, V>(&self, _name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue,
    {
        Box::new(InMemoryManagedMap::new())
    }
}
