use crate::backends::faster_in_memory::{faster_read, faster_rmw, faster_upsert};
use crate::primitives::ManagedValue;
use faster_rs::{status, FasterKv, FasterRmw, FasterValue};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

pub struct FASTERManagedValue<V: 'static + FasterValue + FasterRmw> {
    faster: Arc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    name: String,
    value: PhantomData<V>,
}

impl<V: 'static + FasterValue + FasterRmw> FASTERManagedValue<V> {
    pub fn new(
        faster: Arc<FasterKv>,
        monotonic_serial_number: Rc<RefCell<u64>>,
        name: &str,
    ) -> Self {
        FASTERManagedValue {
            faster,
            monotonic_serial_number,
            name: name.to_owned(),
            value: PhantomData,
        }
    }
}

impl<V: 'static + FasterValue + FasterRmw> ManagedValue<V> for FASTERManagedValue<V> {
    fn set(&mut self, value: V) {
        faster_upsert(
            &self.faster,
            &self.name,
            &value,
            &self.monotonic_serial_number,
        );
    }
    fn get(&self) -> Option<Rc<V>> {
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
