use crate::primitives::{ManagedMap, ManagedValue};
use crate::{StateBackend, StateBackendInfo};

use faster_rs::{status, FasterKey, FasterKv, FasterValue};
use std::cell::RefCell;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

pub struct FASTERBackend {
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
}

impl FASTERBackend {
    fn maybe_refresh(&self, monotonic_serial_number: u64) {
        if monotonic_serial_number % 64 == 0 {
            self.faster.refresh();
            if monotonic_serial_number % 1600 == 0 {
                self.faster.complete_pending(false);
            }
        }
    }
}

impl StateBackend for FASTERBackend {
    fn new(info: &StateBackendInfo) -> Self {
        FASTERBackend {
            faster: Rc::clone(&info.faster),
            monotonic_serial_number: Rc::clone(&info.monotonic_serial_number),
        }
    }

    fn store_count(&mut self, name: &str, count: u64) {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        self.faster
            .upsert(&name, &count, old_monotonic_serial_number);
        self.maybe_refresh(old_monotonic_serial_number);
    }

    fn get_count(&self, name: &str) -> u64 {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        let (status, recv) = self.faster.read(&name, old_monotonic_serial_number);
        if status != status::OK {
            return 0;
        }
        return match recv.recv() {
            Ok(count) => count,
            Err(_) => 0,
        };
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
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        self.faster
            .upsert(&self.name, &value, old_monotonic_serial_number);
    }
    fn get(&mut self) -> Option<V> {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        let (status, recv) = self.faster.read(&self.name, old_monotonic_serial_number);
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
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        self.faster
            .upsert(&key, &value, old_monotonic_serial_number);
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let old_monotonic_serial_number = *self.monotonic_serial_number.borrow();
        *self.monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
        let (status, recv) = self.faster.read(key, old_monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        };
    }
}
