use std::collections::HashMap;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::{StateBackend, StateBackendInfo};
use faster_rs::{FasterKey, FasterValue};
use std::hash::Hash;

pub struct InMemoryBackend {}

impl StateBackend for InMemoryBackend {
    fn new(_: &StateBackendInfo) -> Self {
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

pub struct InMemoryManagedCount {
    count: i64,
}

impl InMemoryManagedCount {
    fn new() -> Self {
        InMemoryManagedCount { count: 0 }
    }
}

impl ManagedCount for InMemoryManagedCount {
    fn decrease(&mut self, amount: i64) {
        self.count -= amount;
    }

    fn increase(&mut self, amount: i64) {
        self.count += amount;
    }

    fn get(&self) -> i64 {
        self.count
    }

    fn set(&mut self, value: i64) {
        self.count = value;
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
    use crate::backends::in_memory::{
        InMemoryManagedCount, InMemoryManagedMap, InMemoryManagedValue,
    };
    use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};

    #[test]
    fn new_in_memory_managed_count_returns_0() {
        let count = InMemoryManagedCount::new();
        assert_eq!(count.get(), 0);
    }

    #[test]
    fn in_memory_managed_count_can_increase() {
        let mut count = InMemoryManagedCount::new();
        count.increase(42);
        assert_eq!(count.get(), 42);
    }

    #[test]
    fn in_memory_managed_count_can_decrease() {
        let mut count = InMemoryManagedCount::new();
        count.decrease(42);
        assert_eq!(count.get(), -42);
    }

    #[test]
    fn in_memory_managed_count_can_set_directly() {
        let mut count = InMemoryManagedCount::new();
        count.set(42);
        assert_eq!(count.get(), 42);
    }

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
