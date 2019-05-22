use std::collections::HashMap;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::{StateBackend, StateBackendInfo};
use faster_rs::{FasterKey, FasterValue};
use std::hash::Hash;
use std::rc::Rc;

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
    value: Option<Rc<V>>,
}

impl<V: 'static + FasterValue> InMemoryManagedValue<V> {
    fn new() -> Self {
        InMemoryManagedValue { value: None }
    }
}

impl<V: 'static + FasterValue> ManagedValue<V> for InMemoryManagedValue<V> {
    fn set(&mut self, value: V) {
        self.value.replace(Rc::new(value));
    }

    fn get(&mut self) -> Option<Rc<V>> {
        match &self.value {
            None => None,
            Some(val) => Some(Rc::clone(val)),
        }
    }

    fn take(&mut self) -> Option<V> {
        match self.value.take() {
            None => None,
            Some(val) => Rc::try_unwrap(val).ok(),
        }
    }

    fn rmw(&mut self, modification: V) {
        self.value = match &self.value {
            None => Some(Rc::new(modification)),
            Some(val) => Some(Rc::new(val.rmw(modification))),
        }
    }
}

pub struct InMemoryManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    map: HashMap<K, Rc<V>>,
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
        self.map.insert(key, Rc::new(value));
    }

    fn get(&mut self, key: &K) -> Option<Rc<V>> {
        match self.map.get(key) {
            None => None,
            Some(val) => Some(Rc::clone(val)),
        }
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        match self.map.remove(key) {
            None => None,
            Some(val) => Rc::try_unwrap(val).ok(),
        }
    }

    fn rmw(&mut self, key: K, modification: V) {
        let new_value = match self.get(&key) {
            None => modification,
            Some(val) => val.rmw(modification),
        };
        self.insert(key, new_value);
    }

    fn contains(&mut self, key: &K) -> bool {
        self.map.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::backends::in_memory::{
        InMemoryManagedCount, InMemoryManagedMap, InMemoryManagedValue,
    };
    use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
    use std::rc::Rc;

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
    fn in_memory_managed_value_take_removes_value() {
        let mut value: InMemoryManagedValue<i32> = InMemoryManagedValue::new();
        value.set(42);
        assert_eq!(value.take(), Some(42));
        assert_eq!(value.take(), None);
    }

    #[test]
    fn in_memory_managed_value_rmw() {
        let mut value: InMemoryManagedValue<i32> = InMemoryManagedValue::new();
        value.set(32);
        value.rmw(10);
        assert_eq!(value.take(), Some(42));
    }

    #[test]
    fn new_in_memory_managed_map_gets_none() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();
        assert_eq!(map.get(&String::from("something")), None);
    }

    #[test]
    fn in_memory_managed_map_remove() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();

        let key = String::from("something");
        let value = 42;

        map.insert(key.clone(), value);
        assert_eq!(map.remove(&key), Some(value));
        assert_eq!(map.get(&key), None);
    }

    #[test]
    fn in_memory_managed_map_rmw() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();

        let key = String::from("something");
        let value = 32;
        let modification = 10;

        map.insert(key.clone(), value);
        map.rmw(key.clone(), modification);
        assert_eq!(map.get(&key), Some(Rc::new(value + modification)));
    }
}
