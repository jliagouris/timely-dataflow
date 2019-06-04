extern crate faster_rs;
extern crate tempfile;

use managed_count::FASTERManagedCount;
use managed_map::FASTERManagedMap;
use managed_value::FASTERManagedValue;

mod managed_count;
mod managed_map;
mod managed_value;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::StateBackend;
use faster_rs::{FasterKey, FasterKv, FasterValue};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use tempfile::TempDir;

#[allow(dead_code)]
pub struct FASTERBackend {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    faster_directory: TempDir,
}

fn maybe_refresh_faster(faster: &Rc<FasterKv>, monotonic_serial_number: u64) {
    if monotonic_serial_number % (1 << 14) == 0 {
        faster.checkpoint().unwrap();
    }
    if monotonic_serial_number % (1 << 8) == 0 {
        faster.complete_pending(false);
    } else if monotonic_serial_number % (1 << 5) == 0 {
        faster.refresh();
    }
}

fn faster_upsert<K: FasterKey, V: FasterValue>(
    faster: &Rc<FasterKv>,
    key: &K,
    value: &V,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    faster.upsert(key, value, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
}

fn faster_read<K: FasterKey, V: FasterValue>(
    faster: &Rc<FasterKv>,
    key: &K,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) -> (u8, Receiver<V>) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    let (status, recv) = faster.read(key, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
    (status, recv)
}

fn faster_rmw<K: FasterKey, V: FasterValue>(
    faster: &Rc<FasterKv>,
    key: &K,
    modification: &V,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    faster.rmw(key, modification, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
}

impl StateBackend for FASTERBackend {
    fn new() -> Self {
        let faster_directory = TempDir::new_in(".").expect("Unable to create directory for FASTER");
        // TODO: check sizing
        let faster_kv = Rc::new(
            FasterKv::new(
                1 << 15,
                4 * 1024 * 1024 * 1024, // 4GB
                faster_directory.path().to_str().unwrap().to_owned(),
            )
            .unwrap(),
        );
        faster_kv.start_session();
        FASTERBackend {
            faster: faster_kv,
            monotonic_serial_number: Rc::new(RefCell::new(1)),
            faster_directory,
        }
    }

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount> {
        Box::new(FASTERManagedCount::new(
            Rc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }

    fn get_managed_value<V: 'static + FasterValue>(&self, name: &str) -> Box<ManagedValue<V>> {
        Box::new(FASTERManagedValue::new(
            Rc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue,
    {
        Box::new(FASTERManagedMap::new(
            Rc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }
}
