use faster_rs::{FasterKey, FasterValue};
pub use managed_count::ManagedCount;
use std::hash::Hash;

pub trait ManagedValue<V: 'static + FasterValue> {
    fn set(&mut self, value: V);
    fn get(&mut self) -> Option<V>;
}

pub trait ManagedMap<K, V>
where
    K: FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    fn insert(&mut self, key: K, value: V);
    fn get(&mut self, key: &K) -> Option<V>;
}

mod managed_count;
