use crate::Rmw;
use std::hash::Hash;
use std::rc::Rc;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait ManagedCount {
    fn decrease(&mut self, amount: i64);
    fn increase(&mut self, amount: i64);
    fn get(&self) -> i64;
    fn set(&mut self, value: i64);
}

pub trait ManagedValue<V: 'static + DeserializeOwned + Serialize + Rmw> {
    fn set(&mut self, value: V);
    fn get(&self) -> Option<Rc<V>>;
    fn take(&mut self) -> Option<V>;
    fn rmw(&mut self, modification: V);
}

pub trait ManagedMap<K, V>
where
    K: 'static + Serialize + Hash + Eq,
    V: 'static + DeserializeOwned + Serialize + Rmw,
{
    fn insert(&mut self, key: K, value: V);
    fn get(&self, key: &K) -> Option<Rc<V>>;
    fn remove(&mut self, key: &K) -> Option<V>;
    fn rmw(&mut self, key: K, modification: V);
    fn contains(&self, key: &K) -> bool;
}
