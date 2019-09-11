use crate::primitives::ManagedCount;
use rocksdb::{WriteBatch, DB};
use std::rc::Rc;
use std::time::{Duration, Instant};

pub struct RocksDBManagedCount {
    db: Rc<DB>,
    name: Vec<u8>,
}

impl RocksDBManagedCount {
    pub fn new(db: Rc<DB>, name: &AsRef<str>) -> Self {
        let start = Instant::now();
        let serialised_name = bincode::serialize(name.as_ref()).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        timing!("serialisation", time_taken);
        timing!("total_serialisation", time_taken);
        RocksDBManagedCount {
            db,
            name: serialised_name,
        }
    }
}

impl ManagedCount for RocksDBManagedCount {
    fn decrease(&mut self, amount: i64) {
        let start = Instant::now();
        let serialised_amount = bincode::serialize(&(-amount)).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        timing!("serialisation", time_taken);
        timing!("total_serialisation", time_taken);
        self.db.merge(&self.name, serialised_amount);
    }

    fn increase(&mut self, amount: i64) {
        let start = Instant::now();
        let serialised_amount = bincode::serialize(&(amount)).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        timing!("serialisation", time_taken);
        timing!("total_serialisation", time_taken);
        self.db.merge(&self.name, serialised_amount);
    }

    fn get(&self) -> i64 {
        let db_vector = self.db.get(&self.name).unwrap();
        match db_vector {
            None => 0,
            Some(db_vector) => {
                let start = Instant::now();
                let value = bincode::deserialize(unsafe {
                    std::slice::from_raw_parts(db_vector.as_ptr(), db_vector.len())
                })
                .unwrap();
                let end = Instant::now();
                let time_taken = end.duration_since(start).subsec_nanos() as u64;
                timing!("deserialisation", time_taken);
                timing!("total_serialisation", time_taken);
                value
            }
        }
    }

    fn set(&mut self, value: i64) {
        let mut batch = WriteBatch::default();
        let start = Instant::now();
        let serialised_value = bincode::serialize(&value).unwrap();
        let end = Instant::now();
        let time_taken = end.duration_since(start).subsec_nanos() as u64;
        timing!("serialisation", time_taken);
        timing!("total_serialisation", time_taken);
        batch.put(&self.name, serialised_value);
        self.db.write_without_wal(batch);
    }
}

#[cfg(test)]
mod tests {
    use super::super::merge_numbers;
    use super::RocksDBManagedCount;
    use crate::primitives::ManagedCount;
    use rocksdb::{Options, DB};
    use std::rc::Rc;
    use tempfile::TempDir;

    #[test]
    fn new_count_returns_0() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_numbers", merge_numbers, Some(merge_numbers));
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let count = RocksDBManagedCount::new(Rc::new(db), &"");
        assert_eq!(count.get(), 0);
    }

    #[test]
    fn count_can_increase() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_numbers", merge_numbers, Some(merge_numbers));
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut count = RocksDBManagedCount::new(Rc::new(db), &"");
        count.increase(42);
        assert_eq!(count.get(), 42);
    }

    #[test]
    fn count_can_decrease() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_numbers", merge_numbers, Some(merge_numbers));
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut count = RocksDBManagedCount::new(Rc::new(db), &"");
        count.decrease(42);
        assert_eq!(count.get(), -42);
    }

    #[test]
    fn count_can_set_directly() {
        let directory = TempDir::new().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("merge_numbers", merge_numbers, Some(merge_numbers));
        let db = DB::open(&options, directory.path()).expect("Unable to instantiate RocksDB");
        let mut count = RocksDBManagedCount::new(Rc::new(db), &"");
        count.set(42);
        assert_eq!(count.get(), 42);
    }
}
