extern crate bincode;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::{StateBackend, StateBackendInfo};

use bincode::serialize;
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
    fn get(&mut self) -> Option<Rc<V>> {
        let (status, recv) = faster_read(&self.faster, &self.name, &self.monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(Rc::new(val)),
            Err(_) => None,
        };
    }

    fn take(&mut self) -> Option<V> {
        let (status, recv) = faster_read(&self.faster, &self.name, &self.monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        };
    }

    fn rmw(&mut self, modification: V) {
        faster_rmw(
            &self.faster,
            &self.name,
            &modification,
            &self.monotonic_serial_number,
        );
    }
}

pub struct FASTERManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    faster: Rc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    serialised_name: Vec<u8>,
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
            serialised_name: serialize(name).unwrap(),
            key: PhantomData,
            value: PhantomData,
        }
    }

    fn prefix_key(&self, key: &K) -> Vec<u8> {
        let mut serialised_key = serialize(key).unwrap();
        let mut prefixed_key = self.serialised_name.clone();
        prefixed_key.append(&mut serialised_key);
        prefixed_key
    }
}

impl<K, V> ManagedMap<K, V> for FASTERManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue,
{
    fn insert(&mut self, key: K, value: V) {
        let prefixed_key = self.prefix_key(&key);
        faster_upsert(
            &self.faster,
            &prefixed_key,
            &value,
            &self.monotonic_serial_number,
        );
    }

    fn get(&mut self, key: &K) -> Option<Rc<V>> {
        let prefixed_key = self.prefix_key(key);
        let (status, recv) =
            faster_read(&self.faster, &prefixed_key, &self.monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(Rc::new(val)),
            Err(_) => None,
        };
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let prefixed_key = self.prefix_key(key);
        let (status, recv) =
            faster_read(&self.faster, &prefixed_key, &self.monotonic_serial_number);
        if status != status::OK {
            return None;
        }
        return match recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        };
    }

    fn rmw(&mut self, key: K, modification: V) {
        let prefixed_key = self.prefix_key(&key);
        faster_rmw(
            &self.faster,
            &prefixed_key,
            &modification,
            &self.monotonic_serial_number,
        );
    }

    fn contains(&mut self, key: &K) -> bool {
        let prefixed_key = self.prefix_key(key);
        let (status, _): (u8, Receiver<V>) =
            faster_read(&self.faster, &prefixed_key, &self.monotonic_serial_number);
        return status == status::OK;
    }
}

#[cfg(test)]
mod tests {
    extern crate faster_rs;
    extern crate tempfile;

    use crate::backends::faster::{FASTERManagedMap, FASTERManagedValue};
    use crate::primitives::{ManagedMap, ManagedValue};
    use faster_rs::FasterKv;
    use std::cell::RefCell;
    use std::rc::Rc;
    use tempfile::TempDir;

    const TABLE_SIZE: u64 = 1 << 14;
    const LOG_SIZE: u64 = 17179869184;

    #[test]
    fn faster_managed_value_set_get() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Rc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let value: u64 = 1337;

        let mut managed_value = FASTERManagedValue::new(store, monotonic_serial_number, "test");
        managed_value.set(value);
        assert_eq!(managed_value.get(), Some(Rc::new(value)));
    }

    #[test]
    fn faster_managed_value_rmw() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Rc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let value: u64 = 1337;
        let modification: u64 = 10;

        let mut managed_value = FASTERManagedValue::new(store, monotonic_serial_number, "test");
        managed_value.set(value);
        managed_value.rmw(modification);
        assert_eq!(managed_value.get(), Some(Rc::new(value + modification)));
    }

    #[test]
    fn faster_managed_map_insert_get() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Rc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let key: u64 = 1;
        let value: u64 = 1337;

        let mut managed_map = FASTERManagedMap::new(store, monotonic_serial_number, "test");
        managed_map.insert(key, value);
        assert_eq!(managed_map.get(&key), Some(Rc::new(value)));
    }

    #[test]
    fn faster_managed_map_contains() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Rc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let key: u64 = 1;
        let value: u64 = 1337;

        let mut managed_map = FASTERManagedMap::new(store, monotonic_serial_number, "test");
        managed_map.insert(key, value);
        assert!(managed_map.contains(&key));
    }

    #[test]
    fn faster_managed_map_rmw() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Rc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let key: u64 = 1;
        let value: u64 = 1337;
        let modification: u64 = 10;

        let mut managed_map = FASTERManagedMap::new(store, monotonic_serial_number, "test");
        managed_map.insert(key, value);
        managed_map.rmw(key, modification);
        assert_eq!(managed_map.get(&key), Some(Rc::new(value + modification)));
    }

    #[test]
    fn faster_managed_map_remove_does_not_remove() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Rc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let key: u64 = 1;
        let value: u64 = 1337;

        let mut managed_map = FASTERManagedMap::new(store, monotonic_serial_number, "test");
        managed_map.insert(key, value);
        assert_eq!(managed_map.remove(&key), Some(value));
        assert_eq!(managed_map.remove(&key), Some(value));
    }
}
