use crate::StateBackend;
use std::cell::RefCell;
use std::rc::Rc;

pub struct ManagedCount<T: StateBackend> {
    backend: Rc<RefCell<T>>,
    name: String,
    count: u64,
}

impl<T: StateBackend> ManagedCount<T> {
    pub fn new(backend: Rc<RefCell<T>>, name: &str) -> Self {
        let current_count = backend.borrow().get_count(name);
        ManagedCount {
            backend,
            name: name.to_owned(),
            count: current_count,
        }
    }

    pub fn decrease(&mut self, amount: u64) {
        self.count -= amount;
        self.backend
            .borrow_mut()
            .store_count(&self.name, self.count);
    }

    pub fn increase(&mut self, amount: u64) {
        self.count += amount;
        self.backend
            .borrow_mut()
            .store_count(&self.name, self.count);
    }

    pub fn get(&self) -> u64 {
        self.count
    }

    pub fn set(&mut self, value: u64) {
        self.count = value;
        self.backend
            .borrow_mut()
            .store_count(&self.name, self.count);
    }
}

#[cfg(test)]
mod tests {
    use crate::backends::InMemoryBackend;
    use crate::primitives::ManagedCount;
    use crate::{StateBackend, StateBackendInfo};
    use faster_rs::FasterKv;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn new_count_returns_zero() {
        let state_backend_info = StateBackendInfo::new(
            FasterKv::new(1 << 14, 17179869184, "/tmp/storage".to_owned()).unwrap(),
        );
        let backend = Rc::new(RefCell::new(InMemoryBackend::new(&state_backend_info)));
        let managed_count = ManagedCount::new(backend.clone(), "count");

        assert_eq!(managed_count.get(), 0);
    }

    #[test]
    fn can_increase_count() {
        let state_backend_info = StateBackendInfo::new(
            FasterKv::new(1 << 14, 17179869184, "/tmp/storage".to_owned()).unwrap(),
        );
        let backend = Rc::new(RefCell::new(InMemoryBackend::new(&state_backend_info)));
        let mut managed_count = ManagedCount::new(backend.clone(), "count");

        assert_eq!(managed_count.get(), 0);

        managed_count.increase(42);

        assert_eq!(managed_count.get(), 42);
    }

    #[test]
    fn can_increase_and_decrease_count() {
        let state_backend_info = StateBackendInfo::new(
            FasterKv::new(1 << 14, 17179869184, "/tmp/storage".to_owned()).unwrap(),
        );
        let backend = Rc::new(RefCell::new(InMemoryBackend::new(&state_backend_info)));
        let mut managed_count = ManagedCount::new(backend.clone(), "count");

        assert_eq!(managed_count.get(), 0);

        managed_count.increase(42);
        managed_count.decrease(12);

        assert_eq!(managed_count.get(), 30);
    }

    #[test]
    #[should_panic]
    fn cannot_decrease_count_below_zero() {
        let state_backend_info = StateBackendInfo::new(
            FasterKv::new(1 << 14, 17179869184, "/tmp/storage".to_owned()).unwrap(),
        );
        let backend = Rc::new(RefCell::new(InMemoryBackend::new(&state_backend_info)));
        let mut managed_count = ManagedCount::new(backend.clone(), "count");

        assert_eq!(managed_count.get(), 0);

        managed_count.decrease(42);
    }

    #[test]
    fn can_set_count_directly() {
        let state_backend_info = StateBackendInfo::new(
            FasterKv::new(1 << 14, 17179869184, "/tmp/storage".to_owned()).unwrap(),
        );
        let backend = Rc::new(RefCell::new(InMemoryBackend::new(&state_backend_info)));
        let mut managed_count = ManagedCount::new(backend.clone(), "count");

        assert_eq!(managed_count.get(), 0);

        managed_count.increase(10);
        managed_count.set(42);

        assert_eq!(managed_count.get(), 42);
    }
}
