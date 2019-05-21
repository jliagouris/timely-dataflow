use std::collections::HashMap;

use crate::primitives::ManagedValue;
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

    fn get_managed_value<V: 'static + FasterValue>(&self, _name: &str) -> Box<ManagedValue<V>> {
        Box::new(InMemoryManagedValue::new())
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

#[cfg(test)]
mod tests {
    use crate::backends::in_memory::InMemoryManagedValue;
    use crate::primitives::ManagedValue;

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
}
