use crate::primitives::ManagedValue;
use faster_rs::FasterValue;
use std::rc::Rc;

pub struct InMemoryManagedValue<V: FasterValue> {
    value: Option<Rc<V>>,
}

impl<V: 'static + FasterValue> InMemoryManagedValue<V> {
    pub fn new() -> Self {
        InMemoryManagedValue { value: None }
    }
}

impl<V: 'static + FasterValue> ManagedValue<V> for InMemoryManagedValue<V> {
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

#[cfg(test)]
mod tests {
    use super::InMemoryManagedValue;
    use crate::primitives::ManagedValue;

    #[test]
    fn new_value_contains_none() {
        let value: InMemoryManagedValue<i32> = InMemoryManagedValue::new();
        assert_eq!(value.get(), None);
    }

    #[test]
    fn value_take_removes_value() {
        let mut value: InMemoryManagedValue<i32> = InMemoryManagedValue::new();
        value.set(42);
        assert_eq!(value.take(), Some(42));
        assert_eq!(value.take(), None);
    }

    #[test]
    fn value_rmw() {
        let mut value: InMemoryManagedValue<i32> = InMemoryManagedValue::new();
        value.set(32);
        value.rmw(10);
        assert_eq!(value.take(), Some(42));
    }

}
