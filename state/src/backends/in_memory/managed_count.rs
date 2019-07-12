use crate::primitives::ManagedCount;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

pub struct InMemoryManagedCount {
    name: String,
    backend: Rc<RefCell<HashMap<String, Rc<Any>>>>,
}

impl InMemoryManagedCount {
    pub fn new(name: &str, backend: Rc<RefCell<HashMap<String, Rc<Any>>>>) -> Self {
        InMemoryManagedCount {
            name: name.to_string(),
            backend,
        }
    }
}

impl ManagedCount for InMemoryManagedCount {
    fn decrease(&mut self, amount: i64) {
        self.set(self.get() - amount);
    }

    fn increase(&mut self, amount: i64) {
        self.set(self.get() + amount);
    }

    fn get(&self) -> i64 {
        let value = match self.backend.borrow_mut().remove(&self.name) {
            None => 0,
            Some(any) => match any.downcast() {
                Ok(count) => Rc::try_unwrap(count).unwrap(),
                Err(_) => 0,
            },
        };
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(value));
        value
    }

    fn set(&mut self, value: i64) {
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(value));
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryManagedCount;
    use crate::primitives::ManagedCount;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    #[test]
    fn new_count_returns_0() {
        let count = InMemoryManagedCount::new("", Rc::new(RefCell::new(HashMap::new())));
        assert_eq!(count.get(), 0);
    }

    #[test]
    fn count_can_increase() {
        let mut count = InMemoryManagedCount::new("", Rc::new(RefCell::new(HashMap::new())));
        count.increase(42);
        assert_eq!(count.get(), 42);
    }

    #[test]
    fn count_can_decrease() {
        let mut count = InMemoryManagedCount::new("", Rc::new(RefCell::new(HashMap::new())));
        count.decrease(42);
        assert_eq!(count.get(), -42);
    }

    #[test]
    fn count_can_set_directly() {
        let mut count = InMemoryManagedCount::new("", Rc::new(RefCell::new(HashMap::new())));
        count.set(42);
        assert_eq!(count.get(), 42);
    }
}
