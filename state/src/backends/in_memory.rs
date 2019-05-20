extern crate bincode;

use std::collections::HashMap;

use crate::{StateBackend, StateBackendInfo};
use bincode::serialize;
use faster_rs::{FasterKey, FasterValue};
use std::any::Any;
use std::rc::Rc;

pub struct InMemoryBackend {
    counts: HashMap<String, u64>,
    values: HashMap<Vec<u8>, Rc<Any>>,
}

impl StateBackend for InMemoryBackend {
    fn new(_: &StateBackendInfo) -> Self {
        InMemoryBackend {
            counts: HashMap::new(),
            values: HashMap::new(),
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

    fn store_value<K, V>(&mut self, key: &K, value: V)
    where
        K: FasterKey,
        V: 'static + FasterValue,
    {
        let serialised_key = serialize(key).unwrap();
        self.values.insert(serialised_key, Rc::new(value));
    }

    fn get_value<K, V>(&mut self, key: &K) -> Option<V>
    where
        K: FasterKey,
        V: 'static + FasterValue,
    {
        let serialised_key = serialize(key).unwrap();
        match self.values.remove(&serialised_key) {
            None => None,
            Some(any) => match any.downcast::<V>() {
                Ok(val) => Rc::try_unwrap(val).ok(),
                Err(_) => None,
            },
        }
    }
}
