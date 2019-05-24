use crate::primitives::ManagedMap;
use faster_rs::{FasterKey, FasterValue};
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

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
    pub fn new() -> Self {
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

    fn get(&self, key: &K) -> Option<Rc<V>> {
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

    fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryManagedMap;
    use crate::primitives::ManagedMap;
    use std::rc::Rc;

    #[test]
    fn new_map_gets_none() {
        let map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();
        assert_eq!(map.get(&String::from("something")), None);
    }

    #[test]
    fn map_remove() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();

        let key = String::from("something");
        let value = 42;

        map.insert(key.clone(), value);
        assert_eq!(map.remove(&key), Some(value));
        assert_eq!(map.get(&key), None);
    }

    #[test]
    fn map_rmw() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();

        let key = String::from("something");
        let value = 32;
        let modification = 10;

        map.insert(key.clone(), value);
        map.rmw(key.clone(), modification);
        assert_eq!(map.get(&key), Some(Rc::new(value + modification)));
    }
}
