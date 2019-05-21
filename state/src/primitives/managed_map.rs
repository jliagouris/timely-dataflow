use crate::StateBackend;
use faster_rs::{FasterKey, FasterValue};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::collections::hash_map::Keys;

pub struct ManagedMap<S, K, V>
where
    S: StateBackend,
    K: FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    backend: Rc<RefCell<S>>,
    name: String,
    keys: HashMap<K, u64>,
    phantom: PhantomData<V>,
    next_id: u64,
}

impl<S: StateBackend, K: FasterKey + Hash + Eq, V: 'static + FasterValue> ManagedMap<S, K, V> {
    pub fn new(backend: Rc<RefCell<S>>, name: &str) -> Self {
        ManagedMap {
            backend,
            name: name.to_owned(),
            keys: HashMap::new(),
            phantom: PhantomData,
            next_id: 0
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let mut physical_name = self.name.clone();
        physical_name.push_str(&self.next_id.to_string());
        self.backend.borrow_mut().store_value(&physical_name, value);
        self.keys.insert(key, self.next_id);
        self.next_id += 1;
    }

    pub fn contains(&self, key: &K) -> bool {
        self.keys.contains_key(key)
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        if !self.keys.contains_key(key) {
            return None;
        };
        let id = self.keys.remove(key).unwrap();
        let mut physical_name = self.name.clone();
        physical_name.push_str(&id.to_string());
        self.backend.borrow_mut().get_value(&physical_name)
    }

    pub fn keys(&self) -> Keys<K, u64> {
        self.keys.keys()
    }

}

#[cfg(test)]
mod tests {
    use crate::backends::InMemoryBackend;
    use crate::primitives::ManagedMap;
    use crate::{StateBackend, StateBackendInfo};
    use faster_rs::FasterKv;
    use std::rc::Rc;
    use std::cell::RefCell;

    #[test]
    fn new_map_gets_none() {
        let state_backend_info = StateBackendInfo::new(
            FasterKv::new(1 << 14, 17179869184, "/tmp/storage".to_owned()).unwrap(),
        );
        let backend = Rc::new(RefCell::new(InMemoryBackend::new(&state_backend_info)));
        let mut managed_map: ManagedMap<_, &str, u64> = ManagedMap::new(backend.clone(), "value");

        assert!(managed_map.get(&"something").is_none());
    }

    #[test]
    fn populated_map_gets_some() {
        let state_backend_info = StateBackendInfo::new(
            FasterKv::new(1 << 14, 17179869184, "/tmp/storage".to_owned()).unwrap(),
        );
        let backend = Rc::new(RefCell::new(InMemoryBackend::new(&state_backend_info)));
        let mut managed_map: ManagedMap<_, &str, u64> = ManagedMap::new(backend.clone(), "value");
        managed_map.insert("something", 42);

        assert_eq!(managed_map.get(&"something"), Some(42));
    }
}
