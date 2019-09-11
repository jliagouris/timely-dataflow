use crate::primitives::ManagedMap;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use rocksdb::{WriteBatch, DB};
use std::hash::Hash;
use std::rc::Rc;
use std::time::Instant;

pub struct RocksDBManagedMap {
    db: Rc<DB>,
    name: Vec<u8>,
}

impl RocksDBManagedMap {
    pub fn new(db: Rc<DB>, name: &AsRef<str>) -> Self {
        let start = Instant::now();
        let serialised_name = bincode::serialize(name.as_ref()).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        timing!("serialisation", time_taken);
        timing!("total_serialisation", time_taken);
        RocksDBManagedMap {
            db,
            name: serialised_name,
        }
    }

    fn prefix_key<K: 'static + FasterKey + Hash + Eq>(&self, key: &K) -> Vec<u8> {
        let start = Instant::now();
        let mut serialised_key = bincode::serialize(key).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        timing!("serialisation", time_taken);
        timing!("total_serialisation", time_taken);
        let mut prefixed_key = self.name.clone();
        prefixed_key.append(&mut serialised_key);
        prefixed_key
    }
}

impl<K, V> ManagedMap<K, V> for RocksDBManagedMap
where
    K: 'static + FasterKey + Hash + Eq,
    V: 'static + FasterValue + FasterRmw,
{
    fn insert(&mut self, key: K, value: V) {
        let prefixed_key = self.prefix_key(&key);
        let mut batch = WriteBatch::default();
        let start = Instant::now();
        let vec = bincode::serialize(&value).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        timing!("serialisation", time_taken);
        timing!("total_serialisation", time_taken);
        batch.put(prefixed_key, vec);
        self.db.write_without_wal(batch);
    }

    fn get(&self, key: &K) -> Option<Rc<V>> {
        let prefixed_key = self.prefix_key(key);
        let db_vector = self.db.get(prefixed_key).unwrap();
        db_vector.map(|db_vector| {
            let start = Instant::now();
            let deserialised = bincode::deserialize(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            });
            let end = Instant::now();
            let time_taken = end.duration_since(start).subsec_nanos() as u64;
            timing!("deserialisation", time_taken);
            timing!("total_serialisation", time_taken);
            Rc::new(deserialised.unwrap())
        })
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let prefixed_key = self.prefix_key(key);
        let db_vector = self.db.get(prefixed_key).unwrap();
        let result = db_vector.map(|db_vector| {
            let start = Instant::now();
            let v = bincode::deserialize(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap();
            let end = Instant::now();
            let time_taken = end.duration_since(start).subsec_nanos() as u64;
            timing!("deserialisation", time_taken);
            timing!("total_serialisation", time_taken);
            v
        });
        self.db.delete(&self.name);
        result
    }

    fn rmw(&mut self, key: K, modification: V) {
        let prefixed_key = self.prefix_key(&key);
        let db_vector = self.db.get(prefixed_key).unwrap();
        let result = db_vector.map(|db_vector| {
            let start = Instant::now();
            let val = bincode::deserialize::<V>(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap();
            let end = Instant::now();
            let time_taken = end.duration_since(start).subsec_nanos() as u64;
            timing!("deserialisation", time_taken);
            timing!("total_serialisation", time_taken);
            val
        });
        let modified = match result {
            Some(val) => val.rmw(modification),
            None => modification,
        };
        self.insert(key, modified);
    }

    fn contains(&self, key: &K) -> bool {
        let prefixed_key = self.prefix_key(key);
        self.db.get(prefixed_key).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::RocksDBManagedMap;
    use crate::primitives::ManagedMap;
    use rocksdb::{Options, DB};
    use std::rc::Rc;
    use tempfile::TempDir;

    #[test]
    fn map_insert_get() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_map = RocksDBManagedMap::new(Rc::new(db), &"");

        let key: u64 = 1;
        let value: u64 = 1337;

        managed_map.insert(key, value);
        assert_eq!(managed_map.get(&key), Some(Rc::new(value)));
    }

    #[test]
    fn map_rmw() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_map = RocksDBManagedMap::new(Rc::new(db), &"");

        let key: u64 = 1;
        let value: u64 = 1337;
        let modification: u64 = 10;

        managed_map.insert(key, value);
        managed_map.rmw(key, modification);
        assert_eq!(managed_map.get(&key), Some(Rc::new(value + modification)));
    }

    #[test]
    fn map_remove_does_not_remove() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_map = RocksDBManagedMap::new(Rc::new(db), &"");

        let key: u64 = 1;
        let value: u64 = 1337;

        managed_map.insert(key, value);
        assert_eq!(managed_map.remove(&key), Some(value));
        assert_eq!(managed_map.remove(&key), Some(value));
    }
}
