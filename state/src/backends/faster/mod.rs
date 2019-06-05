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

pub struct FASTERBackend<'a> {
    faster: &'a FasterKv,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

fn maybe_refresh_faster(faster: &FasterKv, monotonic_serial_number: u64) {
    if monotonic_serial_number % (1 << 14) == 0 {
        let check = faster.checkpoint().unwrap();
        println!("Calling checkpoint with token {}", check.token);
    }
    else if monotonic_serial_number % (1 << 8) == 0 {
        faster.complete_pending(false);
    } else if monotonic_serial_number % (1 << 5) == 0 {
        faster.refresh();
    }
}

fn faster_upsert<K: FasterKey, V: FasterValue>(
    faster: &FasterKv,
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
    faster: &FasterKv,
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
    faster: &FasterKv,
    key: &K,
    modification: &V,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    faster.rmw(key, modification, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
}

impl <'a> StateBackend<'a> for FASTERBackend<'a> {
    type ManagedCounttt = FASTERManagedCount<'a>;

    fn new(faster: &'a FasterKv, monotonic_serial_number: Rc<RefCell<u64>>) -> Self {
        FASTERBackend::<'a> {
            faster,
            monotonic_serial_number: monotonic_serial_number,
        }
    }

    /*
    fn get_managed_count(&self, name: &str) -> Rc<ManagedCount> {
        Rc::new(FASTERManagedCount::new(
            self.faster,
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }
    */
    fn get_managed_count(&self, name: &str) -> FASTERManagedCount<'a> {
        FASTERManagedCount::new(
            self.faster,
            Rc::clone(&self.monotonic_serial_number),
            name,
        )
    }

    fn get_managed_value<V: 'static + FasterValue>(&self, name: &str) -> Box<ManagedValue<V>> {
        unimplemented!();
        /*
        Box::new(FASTERManagedValue::new(
            self.faster,
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
        */
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue,
    {
        unimplemented!();
        /*
        Box::new(FASTERManagedMap::new(
            self.faster,
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
        */
    }
}
