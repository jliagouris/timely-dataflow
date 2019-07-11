use crate::primitives::ManagedMap;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

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
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue + FasterRmw,
{
    pub fn new(name: &str, backend: Rc<RefCell<HashMap<String, Rc<Any>>>>) -> Self {
        let new_map: HashMap<K, V> = HashMap::new();
        backend
            .borrow_mut()
            .insert(name.to_string(), Rc::new(new_map));
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
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue + FasterRmw,
{
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
            Some(rc_any) => match rc_any.downcast() {
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
        let result = inner_map.contains_key(key);
        self.backend
            .borrow_mut()
            .insert(self.name.clone(), Rc::new(inner_map));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryManagedMap;
    use crate::primitives::ManagedMap;
    use std::rc::Rc;

    #[test]
    fn new_map_gets_none() {
        let map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();
        assert_eq!(map.get(&String::from("something")), None);
    }

    #[test]
    fn map_remove() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();

        let key = String::from("something");
        let value = 42;

        map.insert(key.clone(), value);
        assert_eq!(map.remove(&key), Some(value));
        assert_eq!(map.get(&key), None);
    }

    #[test]
    fn map_rmw() {
        let mut map: InMemoryManagedMap<String, i32> = InMemoryManagedMap::new();

        let key = String::from("something");
        let value = 32;
        let modification = 10;

        map.insert(key.clone(), value);
        map.rmw(key.clone(), modification);
        assert_eq!(map.get(&key), Some(Rc::new(value + modification)));
    }
}
