use crate::primitives::ManagedMap;
use faster_rs::{FasterKey, FasterRmw, FasterValue};
use rocksdb::{WriteBatch, DB};
use std::hash::Hash;
use std::rc::Rc;

pub struct RocksDBManagedMap {
    db: Rc<DB>,
    name: Vec<u8>,
}

impl RocksDBManagedMap {
    pub fn new(db: Rc<DB>, name: &AsRef<str>) -> Self {
        RocksDBManagedMap {
            db,
            name: bincode::serialize(name.as_ref()).unwrap(),
        }
    }

    fn prefix_key<K: 'static + FasterKey + Hash + Eq>(&self, key: &K) -> Vec<u8> {
        let mut serialised_key = bincode::serialize(key).unwrap();
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

    fn rmw(&mut self, key: K, modification: V) {
        let prefixed_key = self.prefix_key(&key);
        self.db.merge(&prefixed_key, bincode::serialize(&modification).unwrap());
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
    use rocksdb::MergeOperands;

    fn merge_operator(
        new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: i64 = 0;
        if let Some(val) = existing_val {
            result += bincode::deserialize::<i64>(val).unwrap();
        }
        for operand in operands {
            result += bincode::deserialize::<i64>(operand).unwrap();
        }
        Some(bincode::serialize(&result).unwrap())
    }

    #[test]
    fn map_insert_get() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_operator", merge_operator, Some(merge_operator));
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
        options.set_merge_operator("merge_operator", merge_operator, Some(merge_operator));
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
        options.set_merge_operator("merge_operator", merge_operator, Some(merge_operator));
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
