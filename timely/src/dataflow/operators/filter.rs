//! Filters a stream by a predicate.

use crate::Data;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for filtering.
pub trait Filter<D: Data> {
    /// Returns a new instance of `self` containing only records satisfying `predicate`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Filter, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .filter(|x| *x % 2 == 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn filter(&self, predicate: impl Fn(&D)->bool+'static) -> Self;
}

impl<'a, G: Scope<'a>, D: Data> Filter<D> for Stream<'a, G, D> {
    fn filter(&self, predicate: impl Fn(&D)->bool+'static) -> Stream<'a, G, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Filter", move |_,_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                vector.retain(|x| predicate(x));
                if vector.len() > 0 {
                    output.session(&time).give_vec(&mut vector);
                }
            });
        })
    }
}
