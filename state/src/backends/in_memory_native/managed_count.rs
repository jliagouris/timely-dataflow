use crate::primitives::ManagedCount;

pub struct InMemoryNativeManagedCount {
    count: i64,
}

impl InMemoryNativeManagedCount {
    pub fn new() -> Self {
        InMemoryNativeManagedCount { count: 0 }
    }
}

impl ManagedCount for InMemoryNativeManagedCount {
    fn decrease(&mut self, amount: i64) {
        self.count -= amount;
    }

    fn increase(&mut self, amount: i64) {
        self.count += amount;
    }

    fn get(&self) -> i64 {
        self.count
    }

    fn set(&mut self, value: i64) {
        self.count = value;
    }
}
