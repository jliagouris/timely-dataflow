use crate::backends::faster::{faster_read, faster_rmw, faster_upsert};
use crate::primitives::ManagedMap;
use crate::Rmw;
use bincode::serialize;
use faster_rs::FasterKv;
use std::cell::RefCell;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct FASTERManagedMap
{
    faster: Arc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    serialised_name: Vec<u8>,
}

impl FASTERManagedMap
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
        }
    }

    fn prefix_key<K: Serialize>(&self, key: &K) -> Vec<u8> {
        let mut serialised_key = serialize(key).unwrap();
        let mut prefixed_key = self.serialised_name.clone();
        prefixed_key.append(&mut serialised_key);
        prefixed_key
    }
}

impl<K, V> ManagedMap<K, V> for FASTERManagedMap
where
    K: 'static + Serialize + Hash + Eq,
    V: 'static + DeserializeOwned + Serialize + Rmw,
{
    fn insert(&mut self, key: K, value: V) {
        let prefixed_key = self.prefix_key(&key);
        faster_upsert(
            &self.faster,
            &prefixed_key,
            &bincode::serialize(&value).unwrap(),
            &self.monotonic_serial_number,
        );
    }

    fn get(&self, key: &K) -> Option<Rc<V>> {
        let prefixed_key = self.prefix_key(key);
        let val = faster_read(&self.faster, &prefixed_key, &self.monotonic_serial_number);
        val.map(|v| Rc::new(v))
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let prefixed_key = self.prefix_key(key);
        faster_read(&self.faster, &prefixed_key, &self.monotonic_serial_number)
    }

    fn rmw(&mut self, key: K, modification: V) {
        let prefixed_key = self.prefix_key(&key);
        faster_rmw::<_,_,V>(
            &self.faster,
            &prefixed_key,
            &bincode::serialize(&modification).unwrap(),
            &self.monotonic_serial_number,
        );
    }

    fn contains(&self, key: &K) -> bool {
        let prefixed_key = self.prefix_key(key);
        let val: Option<V> = faster_read(&self.faster, &prefixed_key, &self.monotonic_serial_number);
        val.is_some()
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
