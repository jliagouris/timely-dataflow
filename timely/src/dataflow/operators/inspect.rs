//! Extension trait and implementation for observing and action on streamed data.

use crate::Data;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::generic::Operator;

/// Methods to inspect records and batches of records on a stream.
pub trait Inspect<'a, G: Scope<'a>, D: Data> {
    /// Runs a supplied closure on each observed data element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn inspect(&self, mut func: impl FnMut(&D)+'static) -> Stream<'a, G, D> {
        self.inspect_batch(move |_, data| {
            for datum in data.iter() { func(datum); }
        })
    }

    /// Runs a supplied closure on each observed data element and associated time.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_time(|t, x| println!("seen at: {:?}\t{:?}", t, x));
    /// });
    /// ```
    fn inspect_time(&self, mut func: impl FnMut(&G::Timestamp, &D)+'static) -> Stream<'a, G, D> {
        self.inspect_batch(move |time, data| {
            for datum in data.iter() {
                func(&time, &datum);
            }
        })
    }

    /// Runs a supplied closure on each observed data batch (time and data slice).
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_batch(|t,xs| println!("seen at: {:?}\t{:?} records", t, xs.len()));
    /// });
    /// ```
    fn inspect_batch(&self, func: impl FnMut(&G::Timestamp, &[D])+'static) -> Stream<'a, G, D>;
}

impl<'a, G: Scope<'a>, D: Data> Inspect<'a, G, D> for Stream<'a, G, D> {

    fn inspect_batch(&self, mut func: impl FnMut(&G::Timestamp, &[D])+'static) -> Stream<'a, G, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "InspectBatch", move |_,_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                func(&time, &vector[..]);
                output.session(&time).give_vec(&mut vector);
            });
        })
    }
}
