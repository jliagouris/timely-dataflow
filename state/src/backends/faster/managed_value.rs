use crate::backends::faster::{faster_read, faster_rmw, faster_upsert};
use crate::primitives::ManagedValue;
use crate::Rmw;
use faster_rs::FasterKv;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct FASTERManagedValue {
    faster: Arc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    name: String,
}

impl FASTERManagedValue {
    pub fn new(
        faster: Arc<FasterKv>,
        monotonic_serial_number: Rc<RefCell<u64>>,
        name: &str,
    ) -> Self {
        FASTERManagedValue {
            faster,
            monotonic_serial_number,
            name: name.to_owned(),
        }
    }
}

impl<V: 'static + DeserializeOwned + Serialize + Rmw> ManagedValue<V> for FASTERManagedValue {
    fn set(&mut self, value: V) {
        faster_upsert(
            &self.faster,
            &self.name,
            &bincode::serialize(&value).unwrap(),
            &self.monotonic_serial_number,
        );
    }
    fn get(&self) -> Option<Rc<V>> {
        let val = faster_read(&self.faster, &self.name, &self.monotonic_serial_number);
        val.map(|v| Rc::new(v))
    }

    fn take(&mut self) -> Option<V> {
        faster_read(&self.faster, &self.name, &self.monotonic_serial_number)
    }

    fn rmw(&mut self, modification: V) {
        faster_rmw::<_,_,V>(
            &self.faster,
            &self.name,
            &bincode::serialize(&modification).unwrap(),
            &self.monotonic_serial_number,
        );
    }
}

#[cfg(test)]
mod tests {
    extern crate faster_rs;
    extern crate tempfile;

    use crate::backends::faster::FASTERManagedValue;
    use crate::primitives::ManagedValue;
    use faster_rs::FasterKv;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use tempfile::TempDir;

    const TABLE_SIZE: u64 = 1 << 14;
    const LOG_SIZE: u64 = 17179869184;

    #[test]
    fn value_set_get() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Arc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let value: u64 = 1337;

        let mut managed_value = FASTERManagedValue::new(store, monotonic_serial_number, "test");
        managed_value.set(value);
        assert_eq!(managed_value.get(), Some(Rc::new(value)));
    }

    #[test]
    fn value_rmw() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let store = Arc::new(FasterKv::new(TABLE_SIZE, LOG_SIZE, dir_path).unwrap());
        let monotonic_serial_number = Rc::new(RefCell::new(1));

        let value: u64 = 1337;
        let modification: u64 = 10;

        let mut managed_value = FASTERManagedValue::new(store, monotonic_serial_number, "test");
        managed_value.set(value);
        managed_value.rmw(modification);
        assert_eq!(managed_value.get(), Some(Rc::new(value + modification)));
    }
}
