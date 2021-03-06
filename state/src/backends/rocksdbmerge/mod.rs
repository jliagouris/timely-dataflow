extern crate rocksdb;
use self::rocksdb::BlockBasedOptions;
use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::StateBackend;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use managed_count::RocksDBManagedCount;
use managed_map::RocksDBManagedMap;
use managed_value::RocksDBManagedValue;
use rocksdb::MergeOperands;
use rocksdb::{Options, DB};
use std::hash::Hash;
use std::rc::Rc;
use tempfile::TempDir;

mod managed_count;
mod managed_map;
mod managed_value;

pub struct RocksDBMergeBackend {
    db: Rc<DB>,
}

fn merge_operator(
    new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    // TODO: implement with merge function
    unimplemented!()
}

impl StateBackend for RocksDBMergeBackend {
    fn new() -> Self {
        let directory = TempDir::new_in(".").expect("Unable to create directory for FASTER");
        let mut block_based_options = BlockBasedOptions::default();
        block_based_options.set_block_size(128 * 1024 * 1024); // 128 KB
        block_based_options.set_lru_cache(256 * 1024 * 1024 * 1024); // 256 MB
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_operator", merge_operator, Some(merge_operator));
        options.set_use_fsync(false);
        options.set_min_write_buffer_number(2);
        options.set_max_write_buffer_number(4);
        options.set_write_buffer_size(3 * 1024 * 1024 * 1024); // 3 GB
        options.set_block_based_table_factory(&block_based_options);
        let db = DB::open(&options, directory.into_path()).expect("Unable to instantiate RocksDB");
        RocksDBMergeBackend { db: Rc::new(db) }
    }

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount> {
        Box::new(RocksDBManagedCount::new(Rc::clone(&self.db), &name))
    }

    fn get_managed_value<V: 'static + FasterValue + FasterRmw>(
        &self,
        name: &str,
    ) -> Box<ManagedValue<V>> {
        Box::new(RocksDBManagedValue::new(Rc::clone(&self.db), &name))
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue + FasterRmw,
    {
        Box::new(RocksDBManagedMap::new(Rc::clone(&self.db), &name))
    }
}
