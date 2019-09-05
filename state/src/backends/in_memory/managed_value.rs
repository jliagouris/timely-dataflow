use crate::primitives::ManagedValue;
use crate::Rmw;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct InMemoryManagedValue {
    name: String,
    backend: Rc<RefCell<HashMap<String, Rc<Any>>>>,
}

impl InMemoryManagedValue {
    pub fn new(name: &str, backend: Rc<RefCell<HashMap<String, Rc<Any>>>>) -> Self {
        InMemoryManagedValue {
            name: name.to_string(),
            backend,
        }
    }
}

impl<V: 'static + DeserializeOwned + Serialize + Rmw> ManagedValue<V> for InMemoryManagedValue {
    fn set(&mut self, value: V) {
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(value));
    }

    fn get(&self) -> Option<Rc<V>> {
        let result: Option<V> = match self.backend.borrow_mut().remove(&self.name) {
            None => None,
            Some(value) => match value.downcast::<V>() {
                Ok(value) => Rc::try_unwrap(value).ok(),
                Err(_) => None,
            },
        };
        match result {
            None => None,
            Some(val) => {
                let rc = Rc::new(val);
                let result = Some(Rc::clone(&rc));
                self.backend.borrow_mut().insert(self.name.clone(), rc);
                result
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
        let val: Option<V> = self.take();
        self.set(match val {
            None => modification,
            Some(value) => value.rmw(modification),
        });
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
        let value: InMemoryManagedValue =
            InMemoryManagedValue::new("", Rc::new(RefCell::new(HashMap::new())));
        assert!(value.get().is_none());
    }

    #[test]
    fn value_take_removes_value() {
        let mut value: InMemoryManagedValue =
            InMemoryManagedValue::new("", Rc::new(RefCell::new(HashMap::new())));
        value.set(42);
        assert_eq!(value.take(), Some(42));
        assert_eq!(value.take(), None);
    }

    #[test]
    fn value_rmw() {
        let mut value: InMemoryManagedValue =
            InMemoryManagedValue::new("", Rc::new(RefCell::new(HashMap::new())));
        value.set(32);
        value.rmw(10);
        assert_eq!(value.take(), Some(42));
    }

    #[test]
    fn value_drop() {
        let backend = Rc::new(RefCell::new(HashMap::new()));
        {
            let mut value: InMemoryManagedValue =
                InMemoryManagedValue::new("", Rc::clone(&backend));
            value.set(32);
            value.rmw(10);
            assert_eq!(value.get(), Some(Rc::new(42)));
        }
        {
            let mut value: InMemoryManagedValue =
                InMemoryManagedValue::new("", Rc::clone(&backend));
            assert_eq!(value.take(), Some(42));
        }
    }

}
