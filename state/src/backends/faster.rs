use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::{StateBackend, StateBackendInfo};

use faster_rs::{status, FasterKey, FasterKv, FasterValue};
use std::cell::RefCell;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::mpsc::Receiver;

pub struct FASTERBackend {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

fn maybe_refresh_faster(faster: &Rc<FasterKv>, monotonic_serial_number: u64) {
    if monotonic_serial_number % 64 == 0 {
        faster.refresh();
        if monotonic_serial_number % 1600 == 0 {
            faster.complete_pending(false);
        }
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

pub struct FASTERManagedCount {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    name: String,
}

impl FASTERManagedCount {
    fn new(faster: Rc<FasterKv>, monotonic_serial_number: Rc<RefCell<u64>>, name: &str) -> Self {
        FASTERManagedCount {
            faster,
            monotonic_serial_number,
            name: name.to_owned(),
        }
    }
}

impl ManagedCount for FASTERManagedCount {
    fn decrease(&mut self, amount: i64) {
        faster_rmw(
            &self.faster,
            &self.name,
            &-amount,
            &self.monotonic_serial_number,
        );
    }

    fn increase(&mut self, amount: i64) {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        self.faster
            .rmw(&self.name, &amount, old_monotonic_serial_number);
    }

    fn get(&self) -> i64 {
        let (status, recv) = faster_read(&self.faster, &self.name, &self.monotonic_serial_number);
        if status != status::OK {
            return 0;
        }
        return match recv.recv() {
            Ok(count) => count,
            Err(_) => 0,
        };
    }

    fn set(&mut self, value: i64) {
        faster_upsert(
            &self.faster,
            &self.name,
            &value,
            &self.monotonic_serial_number,
        );
    }
}

pub struct FASTERManagedValue<V: 'static + FasterValue> {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    name: String,
    value: PhantomData<V>,
}

impl<V: 'static + FasterValue> FASTERManagedValue<V> {
    fn new(faster: Rc<FasterKv>, monotonic_serial_number: Rc<RefCell<u64>>, name: &str) -> Self {
        FASTERManagedValue {
            faster,
            monotonic_serial_number,
            name: name.to_owned(),
            value: PhantomData,
        }
    }
}

impl<V: 'static + FasterValue> ManagedValue<V> for FASTERManagedValue<V> {
    fn set(&mut self, value: V) {
        faster_upsert(
            &self.faster,
            &self.name,
            &value,
            &self.monotonic_serial_number,
        );
    }
    fn get(&mut self) -> Option<V> {
        let (status, recv) = faster_read(&self.faster, &self.name, &self.monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        };
    }
}

pub struct FASTERManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    name: String,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K, V> FASTERManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    fn new(faster: Rc<FasterKv>, monotonic_serial_number: Rc<RefCell<u64>>, name: &str) -> Self {
        FASTERManagedMap {
            faster,
            monotonic_serial_number,
            name: name.to_owned(),
            key: PhantomData,
            value: PhantomData,
        }
    }
}

impl<K, V> ManagedMap<K, V> for FASTERManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    fn insert(&mut self, key: K, value: V) {
        faster_upsert(&self.faster, &key, &value, &self.monotonic_serial_number);
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let (status, recv) = faster_read(&self.faster, key, &self.monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        };
    }
}
