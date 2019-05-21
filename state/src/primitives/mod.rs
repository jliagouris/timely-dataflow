use faster_rs::FasterValue;
pub use managed_count::ManagedCount;
pub use managed_map::ManagedMap;

pub trait ManagedValue<V: 'static + FasterValue> {
    fn set(&mut self, value: V);
    fn get(&mut self) -> Option<V>;
}

mod managed_count;
mod managed_map;
