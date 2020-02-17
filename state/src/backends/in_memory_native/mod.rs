use managed_count::InMemoryNativeManagedCount;
use managed_map::InMemoryNativeManagedMap;
use managed_value::InMemoryNativeManagedValue;

mod managed_count;
mod managed_map;
mod managed_value;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::StateBackend;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use std::hash::Hash;

pub struct InMemoryNativeBackend {}

impl StateBackend for InMemoryNativeBackend {
    fn new() -> Self {
        InMemoryNativeBackend {}
    }

    fn get_managed_count(&self, _name: &str) -> Box<ManagedCount> {
        Box::new(InMemoryNativeManagedCount::new())
    }

    fn get_managed_value<V: 'static + FasterValue + FasterRmw>(
        &self,
        _name: &str,
    ) -> Box<ManagedValue<V>> {
        Box::new(InMemoryNativeManagedValue::new())
    }

    fn get_managed_map<K, V>(&self, _name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
        V: 'static + FasterValue + FasterRmw,
    {
        Box::new(InMemoryNativeManagedMap::new())
    }
}
