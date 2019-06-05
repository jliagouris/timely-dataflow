extern crate faster_rs;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use faster_rs::{FasterKey, FasterKv, FasterValue};
use std::cell::{RefCell, Ref};
use std::hash::Hash;
use std::rc::Rc;
use std::marker::PhantomData;

pub mod backends;
pub mod primitives;

pub trait StateBackend<'a> {
    type ManagedCounttt;

    fn new(faster: &'a FasterKv, monotonic_serial_number: Rc<RefCell<u64>>) -> Self;

    //fn get_managed_count(&self, name: &str) -> Rc<ManagedCount>;
    fn get_managed_count(&self, name: &str) -> Self::ManagedCounttt;
    fn get_managed_value<V: 'static + FasterValue>(&self, name: &str) -> Box<ManagedValue<V>>;
    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue;
}

pub struct StateHandle<'a, S: StateBackend<'a>> {
    backend: &'a S,
    name: String,
}

impl<'a, S: StateBackend<'a>> StateHandle<'a, S> {
    pub fn new(backend: &'a S, name: &str) -> Self {
        StateHandle {
            backend,
            name: name.to_owned(),
        }
    }

    pub fn get_managed_count(&self, name: &str) ->  <S as StateBackend<'a>>::ManagedCounttt {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_count(&physical_name)
    }

    pub fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq,
        V: 'static + FasterValue,
    {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_map(&physical_name)
    }

    pub fn get_managed_value<V: 'static + FasterValue>(&self, name: &str) -> Box<ManagedValue<V>> {
        let mut physical_name = self.name.clone();
        physical_name.push_str(name);
        self.backend.get_managed_value(&physical_name)
    }
}
