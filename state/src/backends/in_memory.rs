use std::collections::HashMap;

use crate::StateBackend;

pub struct InMemoryBackend {
    counts: HashMap<String, u64>,
}

impl StateBackend for InMemoryBackend {
    fn new() -> Self {
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
}
