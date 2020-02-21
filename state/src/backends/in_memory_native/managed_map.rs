use crate::primitives::ManagedMap;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::hash::Hash;
use std::rc::Rc;
use rocksdb::DBIterator;

pub struct InMemoryNativeManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
    V: 'static + FasterValue + FasterRmw,
{
    map: HashMap<K, Rc<V>>,
}

impl<K, V> InMemoryNativeManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
    V: 'static + FasterValue + FasterRmw,
{
    pub fn new() -> Self {
        InMemoryNativeManagedMap {
            map: HashMap::new(),
        }
    }
}

impl<K, V> ManagedMap<K, V> for InMemoryNativeManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
    V: 'static + FasterValue + FasterRmw,
{
    fn get_key_prefix_length(&self) -> usize {
        0
    }

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

    fn iter(&mut self, key: K) -> DBIterator {
        panic!("In-memory managed map does not support iteration.");
    }

    fn prefix_iter(&mut self, key: K, key_extractor: Option<&dyn Fn(K) -> K>) -> DBIterator {
        panic!("In-memory managed map does not support iteration.");
    }

    fn next(&mut self, iter: DBIterator) -> Option<(Rc<K>,Rc<V>)> {
        panic!("In-memory managed map does not support iteration.");
    }

    fn delete_range(&mut self, from: K, to: K) {
        panic!("In-Memory backend does not implement delete_range().");
    }
}
