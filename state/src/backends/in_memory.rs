use std::collections::HashMap;

use crate::{StateBackend, StateBackendInfo};
use faster_rs::FasterValue;
use std::any::Any;
use std::rc::Rc;

pub struct InMemoryBackend {
    counts: HashMap<String, u64>,
    values: HashMap<String, Rc<Any>>,
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

    fn store_value<T: 'static + FasterValue>(&mut self, name: &str, value: T) {
        self.values.insert(name.to_owned(), Rc::new(value));
    }

    fn get_value<T: 'static + FasterValue>(&mut self, name: &str) -> Option<T> {
        match self.values.remove(name) {
            None => None,
            Some(any) => match any.downcast::<T>() {
                Ok(val) => Rc::try_unwrap(val).ok(),
                Err(_) => None,
            },
        }
    }
}
