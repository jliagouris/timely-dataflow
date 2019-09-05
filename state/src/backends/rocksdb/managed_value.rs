use crate::primitives::ManagedValue;
use crate::Rmw;
use rocksdb::{WriteBatch, DB};
use std::rc::Rc;
use std::time::Instant;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct RocksDBManagedValue {
    db: Rc<DB>,
    name: Vec<u8>,
}

impl RocksDBManagedValue {
    pub fn new(db: Rc<DB>, name: &AsRef<str>) -> Self {
        RocksDBManagedValue {
            db,
            name: bincode::serialize(name.as_ref()).unwrap(),
        }
    }
}

impl<V: 'static + DeserializeOwned + Serialize + Rmw> ManagedValue<V> for RocksDBManagedValue {
    fn set(&mut self, value: V) {
        let mut batch = WriteBatch::default();
        let start = Instant::now();
        let vec = bincode::serialize(&value).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        counter!("serialisation", time_taken);
        counter!("total_serialisation", time_taken);
        batch.put(&self.name, vec);
        self.db.write_without_wal(batch);
    }

    fn get(&self) -> Option<Rc<V>> {
        let db_vector = self.db.get(&self.name).unwrap();
        db_vector.map(|db_vector| {
            let start = Instant::now();
            let v = bincode::deserialize(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap();
            let end = Instant::now();
            let time_taken = end.duration_since(start).subsec_nanos() as u64;
            counter!("deserialisation", time_taken);
            counter!("total_serialisation", time_taken);
            Rc::new(v)
        })
    }

    fn take(&mut self) -> Option<V> {
        let db_vector = self.db.get(&self.name).unwrap();
        let result = db_vector.map(|db_vector| {
            let start = Instant::now();
            let v = bincode::deserialize(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap();
            let end = Instant::now();
            let time_taken = end.duration_since(start).subsec_nanos() as u64;
            counter!("deserialisation", time_taken);
            counter!("total_serialisation", time_taken);
            v
        });
        self.db.delete(&self.name);
        result
    }

    fn rmw(&mut self, modification: V) {
        let db_vector = self.db.get(&self.name).unwrap();
        let result = db_vector.map(|db_vector| {
            let start = Instant::now();
            let x = bincode::deserialize::<V>(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap();
            let end = Instant::now();
            let time_taken = end.duration_since(start).subsec_nanos() as u64;
            counter!("deserialisation", time_taken);
            counter!("total_serialisation", time_taken);
            x
        });
        let modified = match result {
            Some(val) => val.rmw(modification),
            None => modification,
        };
        self.set(modified);
    }
}

#[cfg(test)]
mod tests {

    use super::RocksDBManagedValue;
    use crate::primitives::ManagedValue;
    use rocksdb::{Options, DB};
    use std::rc::Rc;
    use tempfile::TempDir;

    #[test]
    fn value_set_get() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_value = RocksDBManagedValue::new(Rc::new(db), &"");

        let value: u64 = 1337;
        managed_value.set(value);
        assert_eq!(managed_value.get(), Some(Rc::new(value)));
    }

    #[test]
    fn value_rmw() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_value = RocksDBManagedValue::new(Rc::new(db), &"");

        let value: u64 = 1337;
        let modification: u64 = 10;

        managed_value.set(value);
        managed_value.rmw(modification);
        assert_eq!(managed_value.get(), Some(Rc::new(value + modification)));
    }
}
