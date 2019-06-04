use managed_count::FASTERManagedCount;
use managed_map::FASTERManagedMap;
use managed_value::FASTERManagedValue;

mod managed_count;
mod managed_map;
mod managed_value;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::{StateBackend, StateBackendInfo};
use faster_rs::{FasterKey, FasterKv, FasterValue};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::mpsc::Receiver;

pub struct FASTERBackend {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

fn maybe_refresh_faster(faster: &Rc<FasterKv>, monotonic_serial_number: u64) {
    if monotonic_serial_number % 3200 == 0 {
        let check = faster.checkpoint().unwrap();
        println!("Calling checkpoint with token {}", check.token);
    }
    if monotonic_serial_number % 1600 == 0 {
        faster.complete_pending(true);
    } else if monotonic_serial_number % 64 == 0 {
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
    fn new(info: &StateBackendInfo) -> Self {
        FASTERBackend {
            faster: Rc::clone(&info.faster),
            monotonic_serial_number: Rc::clone(&info.monotonic_serial_number),
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
