use crate::primitives::ManagedCount;
use std::cell::RefCell;
use std::rc::Rc;

pub mod backends;
mod primitives;

pub trait StateBackend: 'static {
    fn new() -> Self;
    fn store_count(&mut self, name: &str, count: u64);
    fn get_count(&self, name: &str) -> u64;
}

pub struct StateHandle<T: StateBackend> {
    backend: Rc<RefCell<T>>,
    name: String,
}

impl<T: StateBackend> StateHandle<T> {
    pub fn new(backend: Rc<RefCell<T>>, name: &str) -> Self {
        StateHandle {
            backend,
            name: name.to_owned(),
        }
    }

    pub fn get_managed_count(&self, name: &str) -> ManagedCount<T> {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        ManagedCount::new(self.backend.clone(), &physical_name)
    }
}
