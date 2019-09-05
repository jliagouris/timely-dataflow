extern crate rocksdb;
use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::{StateBackend, Rmw};
use managed_count::RocksDBManagedCount;
use managed_map::RocksDBManagedMap;
use managed_value::RocksDBManagedValue;
use rocksdb::MergeOperands;
use rocksdb::{Options, DB};
use std::hash::Hash;
use std::rc::Rc;
use tempfile::TempDir;
use serde::de::DeserializeOwned;
use serde::Serialize;

mod managed_count;
mod managed_map;
mod managed_value;

pub struct RocksDBBackend {
    db: Rc<DB>,
}

fn merge_numbers(
    new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: i64 = 0;
    if let Some(val) = existing_val {
        result += bincode::deserialize::<i64>(val).unwrap();
    }
    for operand in operands {
        result += bincode::deserialize::<i64>(operand).unwrap();
    }
    Some(bincode::serialize(&result).unwrap())
}

impl StateBackend for RocksDBBackend {
    fn new() -> Self {
        let directory = TempDir::new_in(".").expect("Unable to create directory for FASTER");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_numbers", merge_numbers, Some(merge_numbers));
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        RocksDBBackend { db: Rc::new(db) }
    }

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount> {
        Box::new(RocksDBManagedCount::new(Rc::clone(&self.db), &name))
    }

    fn get_managed_value<V: 'static + DeserializeOwned + Serialize + Rmw>(
        &self,
        name: &str,
    ) -> Box<ManagedValue<V>> {
        Box::new(RocksDBManagedValue::new(Rc::clone(&self.db), &name))
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + Serialize + Hash + Eq,
        V: 'static + DeserializeOwned + Serialize + Rmw,
    {
        Box::new(RocksDBManagedMap::new(Rc::clone(&self.db), &name))
    }
}