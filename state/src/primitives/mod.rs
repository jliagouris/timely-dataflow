use faster_rs::{FasterKey, FasterRmw, FasterValue};
use std::hash::Hash;
use std::rc::Rc;
use rocksdb::DBIterator;

pub trait ManagedCount {
    fn decrease(&mut self, amount: i64);
    fn increase(&mut self, amount: i64);
    fn get(&self) -> i64;
    fn set(&mut self, value: i64);
}

pub trait ManagedValue<V: 'static + FasterValue + FasterRmw> {
    fn set(&mut self, value: V);
    fn get(&self) -> Option<Rc<V>>;
    fn take(&mut self) -> Option<V>;
    fn rmw(&mut self, modification: V);
}

pub trait ManagedMap<K, V>
where
    K: FasterKey + Hash + Eq + std::fmt::Debug,
    V: 'static + FasterValue + FasterRmw,
{
    fn get_key_prefix_length(&self) -> usize;
    fn insert(&mut self, key: K, value: V);
    fn get(&self, key: &K) -> Option<Rc<V>>;
    fn remove(&mut self, key: &K) -> Option<V>;
    fn rmw(&mut self, key: K, modification: V);
    fn contains(&self, key: &K) -> bool;
    // Implemented only for RocksDB
    fn iter(&mut self, key: K) -> DBIterator;
    fn prefix_iter(&mut self, key: K, key_extractor: Option<&dyn Fn(K) -> K>) -> DBIterator;
    fn next(&mut self, iter: DBIterator) -> Option<(Rc<K>,Rc<V>)>;
    fn delete_range(&mut self, from: K, to: K);
}
