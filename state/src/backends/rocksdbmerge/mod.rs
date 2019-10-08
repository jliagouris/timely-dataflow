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
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::iter::FromIterator;
use std::path::Path;

mod managed_count;
mod managed_map;
mod managed_value;

pub struct RocksDBMergeBackend {
    db: Rc<DB>,
}

// Appends elements to a vector
fn merge_vectors(
    new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
   
   let mut result: Vec<(usize,usize)> = Vec::with_capacity(operands.size_hint().0);

    if let Some(val) = existing_val {
        result.extend(bincode::deserialize::<Vec<(usize,usize)>>(val).unwrap());
    }
    for operand in operands {
        result.extend(bincode::deserialize::<Vec<(usize,usize)>>(operand).unwrap());
    }
    Some(bincode::serialize(&result).unwrap())
}

// read RocksDB configuration from a file
fn read_rocksdb_config() -> (usize, usize, usize, u64) {
    let config_path = String::from("rocksdbmerge.config");
    let file = File::open(config_path).expect("Config file not found or cannot be opened");
    let content = BufReader::new(&file);
    let mut blocksize = 0;
    let mut lrusize = 0;
    let mut write_buffer_size = 0;
    let mut hash_index_size = 0;
    for line in content.lines() {
        let line = line.expect("Could not read the line");
        let line = line.trim();
        if line.starts_with("#") || line.starts_with(";") || line.is_empty() {
            continue;
        }
        let tokens = Vec::from_iter(line.split_whitespace());
        let name = tokens.first().unwrap();
        let tokens = tokens.get(1..).unwrap();
        let tokens = tokens.iter().filter(|t| !t.starts_with("="));
        let tokens = tokens.take_while(|t| !t.starts_with("#") && !t.starts_with(";"));
        let mut parameters = String::new();
        tokens.for_each(|t| { parameters.push_str(t); parameters.push(' '); });
        let parameters = parameters.split(',').map(|s| s.trim());
        let parameters: Vec<String> = parameters.map(|s| s.to_string()).collect();

        // Setting the config parameters
        match name.to_lowercase().as_str() {
            "blocksize" => blocksize = parameters.get(0).unwrap().parse::<usize>().expect("couldn't parse tablesize"),
            "lrusize" => lrusize = parameters.get(0).unwrap().parse::<usize>().expect("couldn't parse logsize"),
            "writebuffersize" => write_buffer_size = parameters.get(0).unwrap().parse::<usize>().expect("couldn't parse writebuffersize"),
            "hashindexsize" => hash_index_size = parameters.get(0).unwrap().parse::<u64>().expect("couldn't parse hashindexsize"),
            _ => (),
        }
    }
    (blocksize, lrusize, write_buffer_size, hash_index_size)
}

impl StateBackend for RocksDBMergeBackend {
    fn new() -> Self {
        let directory = TempDir::new_in(".").expect("Unable to create directory for FASTER");
        let mut block_based_options = BlockBasedOptions::default();
        let (block_size, lru_cache, write_buffer_size, hash_index_size) = read_rocksdb_config();
        println!("Configuring a RocksDB instance with block size {:?}, cache {:?}, write buffer size {:?}, and hash index size {:?}",
                 block_size, lru_cache, write_buffer_size, hash_index_size);
        block_based_options.set_block_size(block_size);
        block_based_options.set_lru_cache(lru_cache);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_vectors", merge_vectors, Some(merge_vectors));
        options.set_use_fsync(false);
        options.set_min_write_buffer_number(2);
        options.set_max_write_buffer_number(4);
        options.set_write_buffer_size(write_buffer_size);
        options.set_block_based_table_factory(&block_based_options);
        options.optimize_for_point_lookup(hash_index_size);
        let db = DB::open(&options, directory.into_path()).expect("Unable to instantiate RocksDBMerge");
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
        K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
        V: 'static + FasterValue + FasterRmw,
    {
        Box::new(RocksDBManagedMap::new(Rc::clone(&self.db), &name))
    }
}
