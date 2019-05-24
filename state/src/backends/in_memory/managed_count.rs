use crate::primitives::ManagedCount;

pub struct InMemoryManagedCount {
    count: i64,
}

impl InMemoryManagedCount {
    pub fn new() -> Self {
        InMemoryManagedCount { count: 0 }
    }
}

impl ManagedCount for InMemoryManagedCount {
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

#[cfg(test)]
mod tests {
    use super::InMemoryManagedCount;
    use crate::primitives::ManagedCount;

    #[test]
    fn new_count_returns_0() {
        let count = InMemoryManagedCount::new();
        assert_eq!(count.get(), 0);
    }

    #[test]
    fn count_can_increase() {
        let mut count = InMemoryManagedCount::new();
        count.increase(42);
        assert_eq!(count.get(), 42);
    }

    #[test]
    fn count_can_decrease() {
        let mut count = InMemoryManagedCount::new();
        count.decrease(42);
        assert_eq!(count.get(), -42);
    }

    #[test]
    fn count_can_set_directly() {
        let mut count = InMemoryManagedCount::new();
        count.set(42);
        assert_eq!(count.get(), 42);
    }
}
