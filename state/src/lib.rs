use std::rc::Rc;

mod backends;
pub use crate::backends::InMemoryBackend;
use std::cell::RefCell;

pub trait StateBackend {
    fn new() -> Self;
    fn store_count(&mut self, name: &str, count: u64);
    fn get_count(&self, name: &str) -> u64;
}

pub struct StateHandle<T: StateBackend> {
    pub backend: Rc<RefCell<T>>,
}

pub struct ManagedCount<T: StateBackend> {
    backend: Rc<RefCell<T>>,
    name: String,
}

impl <T: 'static + StateBackend> StateHandle<T> {
    pub fn get_managed_count(&self, name: &str) -> ManagedCount<T> {
        ManagedCount {
            backend: self.backend.clone(),
            name: name.to_owned(),
        }
    }
}

impl <T: 'static + StateBackend> ManagedCount<T> {
    pub fn incr(&self, amount: u64) {
        self.backend.borrow_mut().store_count(&self.name, amount + 42);
    }
}

