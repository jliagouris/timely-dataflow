use crate::primitives::ManagedValue;
use faster_rs::{FasterRmw, FasterValue};
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;

pub struct InMemoryManagedValue<V: FasterValue + FasterRmw> {
    name: String,
    backend: Rc<RefCell<HashMap<String, Rc<Any>>>>,
    phantom: PhantomData<V>,
}

impl<V: 'static + FasterValue + FasterRmw> InMemoryManagedValue<V> {
    pub fn new(name: &str, backend: Rc<RefCell<HashMap<String, Rc<Any>>>>) -> Self {
        InMemoryManagedValue {
            name: name.to_string(),
            backend,
            phantom: PhantomData,
        }
    }
}

impl<V: 'static + FasterValue + FasterRmw> ManagedValue<V> for InMemoryManagedValue<V> {
    fn set(&mut self, value: V) {
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(value));
    }

    fn get(&self) -> Option<Rc<V>> {
        match self.backend.borrow_mut().remove(&self.name) {
            None => None,
            Some(value) => {
                self.backend
                    .borrow_mut()
                    .insert(self.name.clone(), Rc::clone(&value));
                Some(Rc::clone(&value.downcast().unwrap()))
            }
        }
    }

    fn take(&mut self) -> Option<V> {
        match self.backend.borrow_mut().remove(&self.name) {
            None => None,
            Some(value) => match value.downcast() {
                Ok(value) => Rc::try_unwrap(value).ok(),
                Err(_) => None,
            },
        }
    }

    fn rmw(&mut self, modification: V) {
        match self.take() {
            None => self.set(modification),
            Some(value) => self.set(value.rmw(modification)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryManagedValue;
    use crate::primitives::ManagedValue;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    #[test]
    fn new_value_contains_none() {
        let value: InMemoryManagedValue<i32> =
            InMemoryManagedValue::new("", Rc::new(RefCell::new(HashMap::new())));
        assert_eq!(value.get(), None);
    }

    #[test]
    fn value_take_removes_value() {
        let mut value: InMemoryManagedValue<i32> =
            InMemoryManagedValue::new("", Rc::new(RefCell::new(HashMap::new())));
        value.set(42);
        assert_eq!(value.take(), Some(42));
        assert_eq!(value.take(), None);
    }

    #[test]
    fn value_rmw() {
        let mut value: InMemoryManagedValue<i32> =
            InMemoryManagedValue::new("", Rc::new(RefCell::new(HashMap::new())));
        value.set(32);
        value.rmw(10);
        assert_eq!(value.take(), Some(42));
    }

}
