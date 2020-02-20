use crate::primitives::ManagedMap;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use rocksdb::DBIterator;

pub struct InMemoryManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue + FasterRmw,
{
    name: String,
    backend: Rc<RefCell<HashMap<String, Rc<Any>>>>,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<K, V> InMemoryManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
    V: 'static + FasterValue + FasterRmw,
{
    pub fn new(name: &str, backend: Rc<RefCell<HashMap<String, Rc<Any>>>>) -> Self {
        InMemoryManagedMap {
            name: name.to_string(),
            backend,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
        }
    }
}

impl<K, V> ManagedMap<K, V> for InMemoryManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
    V: 'static + FasterValue + FasterRmw,
{
    fn get_key_prefix_length(&self) -> usize {
        self.name.len()
    }

    fn insert(&mut self, key: K, value: V) {
        let mut inner_map: HashMap<K, Rc<V>> = match self.backend.borrow_mut().remove(&self.name) {
            None => HashMap::new(),
            Some(rc_any) => match rc_any.downcast() {
                Ok(rc_map) => match Rc::try_unwrap(rc_map) {
                    Ok(map) => map,
                    Err(_) => HashMap::new(),
                },
                Err(_) => HashMap::new(),
            },
        };
        inner_map.insert(key, Rc::new(value));
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(inner_map));
    }

    fn get(&self, key: &K) -> Option<Rc<V>> {
        let inner_map: HashMap<K, Rc<V>> = match self.backend.borrow_mut().remove(&self.name) {
            None => HashMap::new(),
            Some(rc_any) => match rc_any.downcast() {
                Ok(rc_map) => match Rc::try_unwrap(rc_map) {
                    Ok(map) => map,
                    Err(_) => HashMap::new(),
                },
                Err(_) => HashMap::new(),
            },
        };
        let result = match inner_map.get(key) {
            None => None,
            Some(val) => Some(Rc::clone(val)),
        };
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(inner_map));
        result
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let mut inner_map: HashMap<K, Rc<V>> = match self.backend.borrow_mut().remove(&self.name) {
            None => HashMap::new(),
            Some(rc_any) => match rc_any.downcast::<HashMap<K, Rc<V>>>() {
                Ok(rc_map) => match Rc::try_unwrap(rc_map) {
                    Ok(map) => map,
                    Err(_) => HashMap::new(),
                },
                Err(_) => HashMap::new(),
            },
        };
        let result = match inner_map.remove(&key) {
            None => None,
            Some(val) => Rc::try_unwrap(val).ok(),
        };
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(inner_map));
        result
    }

    fn rmw(&mut self, key: K, modification: V) {
        let mut inner_map: HashMap<K, Rc<V>> = match self.backend.borrow_mut().remove(&self.name) {
            None => HashMap::new(),
            Some(rc_any) => match rc_any.downcast() {
                Ok(rc_map) => match Rc::try_unwrap(rc_map) {
                    Ok(map) => map,
                    Err(_) => HashMap::new(),
                },
                Err(_) => HashMap::new(),
            },
        };
        let old_value = match inner_map.remove(&key) {
            None => None,
            Some(val) => Rc::try_unwrap(val).ok(),
        };
        match old_value {
            None => inner_map.insert(key, Rc::new(modification)),
            Some(val) => inner_map.insert(key, Rc::new(val.rmw(modification))),
        };
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(inner_map));
    }

    fn contains(&self, key: &K) -> bool {
        let inner_map: HashMap<K, Rc<V>> = match self.backend.borrow_mut().remove(&self.name) {
            None => HashMap::new(),
            Some(rc_any) => match rc_any.downcast() {
                Ok(rc_map) => match Rc::try_unwrap(rc_map) {
                    Ok(map) => map,
                    Err(_) => HashMap::new(),
                },
                Err(_) => HashMap::new(),
            },
        };
        let result = inner_map.contains_key(key);
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(inner_map));
        result
    }

    fn iter(&mut self, key: K) -> DBIterator {
        panic!("In-memory managed map does not support iteration.");
    }

    fn prefix_iter(&mut self, key: K, key_extractor: Option<&dyn Fn(K) -> K>) -> DBIterator {
        panic!("In-memory managed map does not support iteration.");
    }

    fn next(&mut self, iter: DBIterator) -> Option<(Rc<K>,Rc<V>)> {
        panic!("In-memory managed map does not support iteration.");
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryManagedMap;
    use crate::primitives::ManagedMap;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    #[test]
    fn new_map_gets_none() {
        let map: InMemoryManagedMap<String, i32> =
            InMemoryManagedMap::new("", Rc::new(RefCell::new(HashMap::new())));
        assert_eq!(map.get(&String::from("something")), None);
    }

    #[test]
    fn map_remove() {
        let mut map: InMemoryManagedMap<String, i32> =
            InMemoryManagedMap::new("", Rc::new(RefCell::new(HashMap::new())));

        let key = String::from("something");
        let value = 42;

        map.insert(key.clone(), value);
        assert_eq!(map.remove(&key), Some(value));
        assert_eq!(map.get(&key), None);
    }

    #[test]
    fn map_rmw() {
        let mut map: InMemoryManagedMap<String, i32> =
            InMemoryManagedMap::new("", Rc::new(RefCell::new(HashMap::new())));

        let key = String::from("something");
        let value = 32;
        let modification = 10;

        map.insert(key.clone(), value);
        map.rmw(key.clone(), modification);
        assert_eq!(map.get(&key), Some(Rc::new(value + modification)));
    }

    #[test]
    fn map_drop() {
        let backend = Rc::new(RefCell::new(HashMap::new()));
        {
            let mut map: InMemoryManagedMap<String, i32> =
                InMemoryManagedMap::new("state", Rc::clone(&backend));
            map.insert("hello".to_string(), 100);
            map.rmw("hello".to_string(), 50);
            assert_eq!(
                Rc::new(150),
                map.get(&"hello".to_string())
                    .expect("Value not rmw correctly")
            );
        }
        {
            let mut map: InMemoryManagedMap<String, i32> =
                InMemoryManagedMap::new("state", Rc::clone(&backend));
            assert_eq!(
                150,
                map.remove(&"hello".to_string())
                    .expect("Value dropped from backend")
            );
        }
    }
}
