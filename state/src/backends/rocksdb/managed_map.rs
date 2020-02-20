use crate::primitives::ManagedMap;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use rocksdb::{DBIterator, Direction, IteratorMode, WriteBatch, DB};
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

pub struct RocksDBManagedMap<K, V> {
    db: Rc<DB>,
    name: Vec<u8>,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K: 'static + FasterKey + Hash + Eq + std::fmt::Debug, V: 'static + FasterValue + FasterRmw>
    RocksDBManagedMap<K, V>
{
    pub fn new(db: Rc<DB>, name: &AsRef<str>) -> Self {
        RocksDBManagedMap {
            db,
            name: bincode::serialize(name.as_ref()).unwrap(),
            key: PhantomData,
            value: PhantomData,
        }
    }

    fn prefix_key(&self, key: &K) -> Vec<u8> {
        let mut serialised_key = bincode::serialize(key).unwrap();
        let mut prefixed_key = self.name.clone();
        prefixed_key.append(&mut serialised_key);
        prefixed_key
    }

}

impl<K, V> ManagedMap<K, V> for RocksDBManagedMap<K, V>
where
    K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
    V: 'static + FasterValue + FasterRmw,
{
    fn get_key_prefix_length(&self) -> usize {
        self.name.len()
    }

    fn insert(&mut self, key: K, value: V) {
        let prefixed_key = self.prefix_key(&key);
        let mut batch = WriteBatch::default();
        batch.put(prefixed_key, bincode::serialize(&value).unwrap());
        self.db.write_without_wal(batch);
    }

    fn get(&self, key: &K) -> Option<Rc<V>> {
        let prefixed_key = self.prefix_key(key);
        let db_vector = self.db.get(prefixed_key).unwrap();
        db_vector.map(|db_vector| {
            Rc::new(
                bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
                })
                .unwrap(),
            )
        })
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let prefixed_key = self.prefix_key(key);
        let db_vector = self.db.get(prefixed_key).unwrap();
        let result = db_vector.map(|db_vector| {
            bincode::deserialize(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap()
        });
        self.db.delete(&self.name);
        result
    }

    // Updates values using get+put
    fn rmw(&mut self, key: K, modification: V) {
        let prefixed_key = self.prefix_key(&key);
        let db_vector = self.db.get(prefixed_key).unwrap();
        let result = db_vector.map(|db_vector| {
            bincode::deserialize::<V>(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap()
        });
        let modified = match result {
            Some(val) => val.rmw(modification),
            None => modification,
        };
        self.insert(key, modified);
    }

    // Returns a forward DBIterator starting from 'key'
    fn iter(&mut self, key: K, key_extractor: Option<&dyn Fn(K) -> K>) -> DBIterator {
        let prefixed_key = self.prefix_key(&key);
        self.db.iterator(IteratorMode::From(&prefixed_key, Direction::Forward))
    }

    // Returns the next value of the given DBIterator
    fn next(&mut self, mut iter: DBIterator) -> Option<(Rc<K>,Rc<V>)> {
        if let Some((raw_key, raw_value)) = iter.next() {
            let key = Rc::new(
                bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(raw_key.as_ptr(), raw_key.len())
                })
                .unwrap(),
            );
            let value = Rc::new(
                bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(raw_value.as_ptr(), raw_value.len())
                })
                .unwrap(),
            );
            return Some((key, value));
        }
        None
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
    use bincode;
    use rocksdb::{DBIterator, Options, DB};
    use std::convert::TryFrom;
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

    #[test]
    fn db_iterate() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_map = RocksDBManagedMap::new(Rc::new(db), &"");
        let prefix_length = managed_map.get_key_prefix_length();

        let key: u64 = 1;
        let value: u64 = 1337;
        let key_2: u64 = 2;
        let value_2: u64 = 1338;
        let key_3: u64 = 3;
        let value_3: u64 = 1333;

        managed_map.insert(key, value);
        managed_map.insert(key_2, value_2);
        managed_map.insert(key_3, value_3);

        // Get the iterator
        let mut iter = managed_map.iter(key);

        // TODO (john): Deserialization should be transparent. The following is ugly but ok for now.

        // Start iterating
        let (k, _) = iter.next().unwrap();
        let kk = &k[prefix_length..];  // Ignore prefix
        let ki: u64 = bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(kk.as_ptr(), kk.len())
                }).unwrap();
        assert_eq!(ki, key);
        let (k2, _) = iter.next().unwrap();
        let kk2 = &k2[prefix_length..];  // Ignore prefix
        let ki2: u64 = bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(kk2.as_ptr(), kk.len())
                }).unwrap();
        assert_eq!(ki2, key_2);
        let (k3, _) = iter.next().unwrap();
        let kk3 = &k3[prefix_length..];  // Ignore prefix
        let ki3: u64 = bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(kk3.as_ptr(), kk.len())
                }).unwrap();
        assert_eq!(ki3, key_3);

        // Verify end of iteration
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn db_contains() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_map = RocksDBManagedMap::new(Rc::new(db), &"");

        let key: u64 = 1;
        let value: u64 = 1337;
        let key_2: u64 = 2;
        let value_2: u64 = 1338;
        let key_3: u64 = 3;
        let value_3: u64 = 1333;
        managed_map.insert(key, value);
        assert_eq!(managed_map.contains(&key), true);
    }
}
