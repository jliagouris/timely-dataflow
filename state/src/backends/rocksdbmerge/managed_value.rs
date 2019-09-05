use crate::primitives::ManagedValue;
use crate::Rmw;
use rocksdb::{WriteBatch, DB};
use std::rc::Rc;
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
        batch.put(&self.name, bincode::serialize(&value).unwrap());
        self.db.write_without_wal(batch);
    }

    fn get(&self) -> Option<Rc<V>> {
        let db_vector = self.db.get(&self.name).unwrap();
        db_vector.map(|db_vector| {
            Rc::new(
                bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
                })
                .unwrap(),
            )
        })
    }

    fn take(&mut self) -> Option<V> {
        let db_vector = self.db.get(&self.name).unwrap();
        let result = db_vector.map(|db_vector| {
            bincode::deserialize(unsafe {
                std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
            })
            .unwrap()
        });
        self.db.delete(&self.name);
        result
    }

    fn rmw(&mut self, modification: V) {
        self.db.merge(&self.name, bincode::serialize(&modification).unwrap());
    }
}

#[cfg(test)]
mod tests {

    use super::RocksDBManagedValue;
    use crate::primitives::ManagedValue;
    use rocksdb::{Options, DB, MergeOperands};
    use std::rc::Rc;
    use tempfile::TempDir;

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
    fn value_set_get() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_operator", merge_operator, Some(merge_operator));
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
        options.set_merge_operator("merge_operator", merge_operator, Some(merge_operator));
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut managed_value = RocksDBManagedValue::new(Rc::new(db), &"");

        let value: u64 = 1337;
        let modification: u64 = 10;

        managed_value.set(value);
        managed_value.rmw(modification);
        assert_eq!(managed_value.get(), Some(Rc::new(value + modification)));
    }
}
