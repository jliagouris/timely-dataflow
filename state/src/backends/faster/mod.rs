extern crate faster_rs;
extern crate tempfile;

use managed_count::FASTERManagedCount;
use managed_map::FASTERManagedMap;
use managed_value::FASTERManagedValue;

mod managed_count;
mod managed_map;
mod managed_value;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::{StateBackend, Rmw};
use faster_rs::{FasterKv, FasterKvBuilder};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::iter::FromIterator;
use std::path::Path;

pub struct FASTERBackend {
    faster: Arc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

fn maybe_refresh_faster(faster: &Arc<FasterKv>, monotonic_serial_number: u64) {
    if monotonic_serial_number % (1 << 4) == 0 {
        faster.refresh();
        if monotonic_serial_number % (1 << 10) == 0 {
            faster.complete_pending(true);
        }
    }
    if monotonic_serial_number % (1 << 20) == 0 {
        println!("Size: {}", faster.size());
    }
}

fn faster_upsert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    faster: &Arc<FasterKv>,
    key: K,
    value: V,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    faster.upsert(&key, &value, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
}

fn faster_read<K: AsRef<[u8]>, V: DeserializeOwned>(
    faster: &Arc<FasterKv>,
    key: K,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) -> Option<V> {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    let (status, recv) = faster.read(key, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
    recv.recv().ok().map(|vec| {
        let start = Instant::now();
        let deserialised = bincode::deserialize(&vec).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        counter!("deserialisation", time_taken);
        counter!("total_serialisation", time_taken);
        deserialised
    })
}

fn faster_rmw<K: AsRef<[u8]>, V: AsRef<[u8]>, R: DeserializeOwned + Serialize + Rmw>(
    faster: &Arc<FasterKv>,
    key: K,
    modification: V,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    faster.rmw(key, &modification, rmw_logic::<R>, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
}

fn rmw_logic<V: DeserializeOwned + Serialize + Rmw>(val: &[u8], modif: &[u8]) -> Vec<u8> {
    let start = Instant::now();
    let val: V = bincode::deserialize(val).unwrap();
    let modif = bincode::deserialize(modif).unwrap();
    let end = Instant::now();
    let time_taken = end.duration_since(start).subsec_nanos() as u64;
    counter!("deserialisation", time_taken);
    counter!("total_serialisation", time_taken);
    let modified = val.rmw(modif);
    let start = Instant::now();
    let val = bincode::serialize(&modified).unwrap();
    let end = Instant::now();
    let time_taken = end.duration_since(start).subsec_nanos() as u64;
    counter!("serialisation", time_taken);
    counter!("total_serialisation", time_taken);
    val
}

// read faster configuration from a file
fn read_faster_config() -> (u64, u64) {
    let config_path = String::from("faster.config");
    let file = File::open(config_path).expect("Config file not found or cannot be opened");
    let content = BufReader::new(&file);
    let mut tablesize = 0;
    let mut logsize = 0;
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
            "tablesize" => tablesize = parameters.get(0).unwrap().parse::<u64>().expect("couldn't parse tablesize"),
            "logsize" => logsize = parameters.get(0).unwrap().parse::<u64>().expect("couldn't parse logsize"),
            _ => (),
        }
    }
    (tablesize, logsize)
}

impl StateBackend for FASTERBackend {
    fn new() -> Self {
        let faster_directory = TempDir::new_in(".")
            .expect("Unable to create directory for FASTER")
            .into_path();
        let faster_directory_string = faster_directory.to_str().unwrap();
        // TODO: check sizing
        let (tablesize, logsize) = read_faster_config();
        println!("Configuring a FASTER instance with hash index {:?} and log size {:?}", tablesize, logsize);
        let mut builder = FasterKvBuilder::new(tablesize, logsize);
        builder
            .with_disk(faster_directory_string)
            .set_pre_allocate_log(true);
        let faster_kv = Arc::new(builder.build().unwrap());
        faster_kv.start_session();
        FASTERBackend {
            faster: faster_kv,
            monotonic_serial_number: Rc::new(RefCell::new(1)),
        }
    }

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount> {
        Box::new(FASTERManagedCount::new(
            Arc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }

    fn get_managed_value<V: 'static + DeserializeOwned + Serialize + Rmw>(
        &self,
        name: &str,
    ) -> Box<ManagedValue<V>> {
        Box::new(FASTERManagedValue::new(
            Arc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + Serialize + Hash + Eq + std::fmt::Debug,
        V: 'static + DeserializeOwned + Serialize + Rmw,
    {
        Box::new(FASTERManagedMap::new(
            Arc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }
}

impl FASTERBackend {
    pub fn new_from_existing(faster_kv: &Arc<FasterKv>) -> Self {
        FASTERBackend {
            faster: Arc::clone(faster_kv),
            monotonic_serial_number: Rc::new(RefCell::new(1)),
        }
    }
}
