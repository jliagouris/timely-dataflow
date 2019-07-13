use crate::primitives::ManagedValue;
use faster_rs::{FasterRmw, FasterValue};
use std::rc::Rc;

pub struct InMemoryNativeManagedValue<V: FasterValue + FasterRmw> {
    value: Option<Rc<V>>,
}

impl<V: 'static + FasterValue + FasterRmw> InMemoryNativeManagedValue<V> {
    pub fn new() -> Self {
        InMemoryNativeManagedValue { value: None }
    }
}

impl<V: 'static + FasterValue + FasterRmw> ManagedValue<V> for InMemoryNativeManagedValue<V> {
    fn set(&mut self, value: V) {
        self.value.replace(Rc::new(value));
    }

    fn get(&self) -> Option<Rc<V>> {
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
