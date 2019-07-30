use crate::backends::faster_node::{faster_read, faster_rmw, faster_upsert};
use crate::primitives::ManagedMap;
use bincode::serialize;
use faster_rs::{status, FasterKey, FasterKv, FasterRmw, FasterValue};
use std::cell::RefCell;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::Arc;

pub struct FASTERManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue + FasterRmw,
{
    faster: Arc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    serialised_name: Vec<u8>,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K, V> FASTERManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue + FasterRmw,
{
    pub fn new(
        faster: Arc<FasterKv>,
        monotonic_serial_number: Rc<RefCell<u64>>,
        name: &str,
    ) -> Self {
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
    V: 'static + FasterValue + FasterRmw,
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

    fn get(&self, key: &K) -> Option<Rc<V>> {
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

    fn contains(&self, key: &K) -> bool {
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

    use crate::backends::faster::FASTERManagedMap;
    use crate::primitives::ManagedMap;
    use faster_rs::FasterKv;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use tempfile::TempDir;

    const TABLE_SIZE: u64 = 1 << 14;
    const LOG_SIZE: u64 = 17179869184;

    #[test]
    fn map_insert_get() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Arc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let key: u64 = 1;
        let value: u64 = 1337;

        let mut managed_map = FASTERManagedMap::new(store, monotonic_serial_number, "test");
        managed_map.insert(key, value);
        assert_eq!(managed_map.get(&key), Some(Rc::new(value)));
    }

    #[test]
    fn map_contains() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Arc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let key: u64 = 1;
        let value: u64 = 1337;

        let mut managed_map = FASTERManagedMap::new(store, monotonic_serial_number, "test");
        managed_map.insert(key, value);
        assert!(managed_map.contains(&key));
    }

    #[test]
    fn map_rmw() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Arc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
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
    fn map_remove_does_not_remove() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Arc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let key: u64 = 1;
        let value: u64 = 1337;

        let mut managed_map = FASTERManagedMap::new(store, monotonic_serial_number, "test");
        managed_map.insert(key, value);
        assert_eq!(managed_map.remove(&key), Some(value));
        assert_eq!(managed_map.remove(&key), Some(value));
    }
}
