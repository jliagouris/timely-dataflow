use faster_rs::{FasterKey, FasterValue};
use std::hash::Hash;

pub trait ManagedCount {
    fn decrease(&mut self, amount: i64);
    fn increase(&mut self, amount: i64);
    fn get(&self) -> i64;
    fn set(&mut self, value: i64);
}

pub trait ManagedValue<V: 'static + FasterValue> {
    fn set(&mut self, value: V);
    fn get(&mut self) -> Option<V>;
    fn rmw(&mut self, modification: V);
}

pub trait ManagedMap<K, V>
where
    K: FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    fn insert(&mut self, key: K, value: V);
    fn get(&mut self, key: &K) -> Option<V>;
    fn rmw(&mut self, key: K, modification: V);
    fn contains(&mut self, key: &K) -> bool;
}
