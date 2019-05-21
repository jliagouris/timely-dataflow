use std::collections::HashMap;

use crate::primitives::{ManagedMap, ManagedValue};
use crate::{StateBackend, StateBackendInfo};
use faster_rs::{FasterKey, FasterValue};
use std::any::Any;
use std::hash::Hash;
use std::rc::Rc;

pub struct InMemoryBackend {
    counts: HashMap<String, u64>,
}

impl StateBackend for InMemoryBackend {
    fn new(_: &StateBackendInfo) -> Self {
        InMemoryBackend {
            counts: HashMap::new(),
        }
    }
    fn store_count(&mut self, name: &str, count: u64) {
        self.counts.insert(name.to_owned(), count);
    }
    fn get_count(&self, name: &str) -> u64 {
        match self.counts.get(name) {
            None => 0,
            Some(count) => *count,
        }
    }

    fn get_managed_value<V: 'static + FasterValue>(&self, _name: &str) -> Box<ManagedValue<V>> {
        Box::new(InMemoryManagedValue::new())
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue,
    {
        Box::new(InMemoryManagedMap::new())
    }
}

pub struct InMemoryManagedValue<V: FasterValue> {
    value: Option<V>,
}

impl<V: 'static + FasterValue> InMemoryManagedValue<V> {
    fn new() -> Self {
        InMemoryManagedValue { value: None }
    }
}

impl<V: 'static + FasterValue> ManagedValue<V> for InMemoryManagedValue<V> {
    fn set(&mut self, value: V) {
        self.value = Some(value);
    }
    fn get(&mut self) -> Option<V> {
        self.value.take()
    }
}

pub struct InMemoryManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    map: HashMap<K, V>,
}

impl<K, V> InMemoryManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    fn new() -> Self {
        InMemoryManagedMap {
            map: HashMap::new(),
        }
    }
}

impl<K, V> ManagedMap<K, V> for InMemoryManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    fn insert(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    fn get(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::backends::in_memory::{InMemoryManagedMap, InMemoryManagedValue};
    use crate::primitives::{ManagedMap, ManagedValue};

    #[test]
    fn new_in_memory_managed_value_contains_none() {
        let mut value: InMemoryManagedValue<i32> = InMemoryManagedValue::new();
        assert_eq!(value.get(), None);
    }

    #[test]
    fn in_memory_managed_value_returns_some_then_returns_none() {
        let mut value: InMemoryManagedValue<i32> = InMemoryManagedValue::new();
        value.set(42);
        assert_eq!(value.get(), Some(42));
        assert_eq!(value.get(), None);
    }

    #[test]
    fn new_in_memory_managed_map_gets_none() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();
        assert_eq!(map.get(&String::from("something")), None);
    }

    #[test]
    fn in_memory_managed_map_gets_some_then_gets_none() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();

        let key = String::from("something");
        let value = 42;

        map.insert(key.clone(), value);
        assert_eq!(map.get(&key), Some(value));
        assert_eq!(map.get(&key), None);
    }
}
