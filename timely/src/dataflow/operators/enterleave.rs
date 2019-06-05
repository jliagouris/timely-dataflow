//! Extension traits to move a `Stream` between an outer `Scope` and inner `Scope`.
//!
//! Each `Stream` indicates its containing `Scope` as part of its type signature. To create a new
//! stream with the same contents in another scope, one must explicit use the methods `enter` and
//! `leave`, to clearly indicate the transition to the timely dataflow progress tracking logic.
//!
//! # Examples
//! ```
//! use timely::dataflow::scopes::Scope;
//! use timely::dataflow::operators::{Enter, Leave, ToStream, Inspect};
//!
//! timely::example(|outer| {
//!     let stream = (0..9).to_stream(outer);
//!     let output = outer.region(|inner| {
//!         stream.enter(inner)
//!               .inspect(|x| println!("in nested scope: {:?}", x))
//!               .leave()
//!     });
//! });
//! ```

// use std::default::Default;

use std::marker::PhantomData;

use crate::progress::Timestamp;
use crate::progress::timestamp::Refines;
use crate::progress::{Source, Target};
use crate::order::Product;
use crate::Data;
use crate::communication::Push;
use crate::dataflow::channels::pushers::{Counter, Tee};
use crate::dataflow::channels::{Bundle, Message};

use crate::worker::AsWorker;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::scopes::{Child, ScopeParent};
use crate::dataflow::operators::delay::Delay;

/// Extension trait to move a `Stream` into a child of its current `Scope`.
pub trait Enter<'a, G: Scope<'a>, T: Timestamp+Refines<G::Timestamp>, D: Data> {
    /// Moves the `Stream` argument into a child of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn enter<'b>(&self, _: &Child<'b, G, T, G::StateBackend>) -> Stream<'a, Child<'b, G, T, G::StateBackend>, D>
    where
        'a:'b;
}

use crate::dataflow::scopes::child::Iterative;

/// Extension trait to move a `Stream` into a child of its current `Scope` setting the timestamp for each element.
pub trait EnterAt<'a, G: Scope<'a>, T: Timestamp, D: Data> {
    /// Moves the `Stream` argument into a child of its current `Scope` setting the timestamp for each element by `initial`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{EnterAt, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9u64).to_stream(outer);
    ///     let output = outer.iterative(|inner| {
    ///         stream.enter_at(inner, |x| *x).leave()
    ///     });
    /// });
    /// ```
    fn enter_at<'b, F:Fn(&D)->T+'static>(&self, scope: &Iterative<'b, G, T, G::StateBackend>, initial: F) -> Stream<'a, Iterative<'b, G, T, G::StateBackend>, D>
    where
        'a:'b;
}

impl<'a, G: Scope<'a>, T: Timestamp, D: Data, E: Enter<'a, G, Product<<G as ScopeParent>::Timestamp, T>, D>> EnterAt<'a, G, T, D> for E {
    fn enter_at<'b, F:Fn(&D)->T+'static>(&self, scope: &Iterative<'b, G, T, G::StateBackend>, initial: F) ->
        Stream<'a, Iterative<'b, G, T, G::StateBackend>, D>
    where
        'a:'b
    {
            self.enter(scope).delay(move |datum, time| Product::new(time.clone().to_outer(), initial(datum)))
    }
}

impl<'a, G: Scope<'a>, T: Timestamp+Refines<G::Timestamp>, D: Data> Enter<'a, G, T, D> for Stream<'a, G, D> {
    fn enter<'b>(&self, scope: &Child<'b, G, T, G::StateBackend>) -> Stream<'a, Child<'b, G, T, G::StateBackend>, D>
        where
            'a:'b
    {

        let (targets, registrar) = Tee::<T, D>::new();
        let ingress = IngressNub { targets: Counter::new(targets), phantom: ::std::marker::PhantomData };
        let produced = ingress.targets.produced().clone();

        let input = scope.subgraph.borrow_mut().new_input(produced);

        let channel_id = scope.clone().new_identifier();
        self.connect_to(input, ingress, channel_id);
        Stream::new(Source { index: 0, port: input.port }, registrar, scope.clone())
    }
}

/// Extension trait to move a `Stream` to the parent of its current `Scope`.
pub trait Leave<'a, G: Scope<'a>, D: Data> {
    /// Moves a `Stream` to the parent of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn leave(&self) -> Stream<'a, G, D>;
}

impl<'a, 'b, G: Scope<'a>, D: Data, T: Timestamp+Refines<G::Timestamp>> Leave<'a, G, D> for Stream<'a, Child<'b, G, T, G::StateBackend>, D>
where
    'a: 'b
{
    fn leave(&self) -> Stream<'a, G, D> {

        let scope = self.scope();

        let output = scope.subgraph.borrow_mut().new_output();
        let (targets, registrar) = Tee::<G::Timestamp, D>::new();
        let channel_id = scope.clone().new_identifier();
        self.connect_to(Target { index: 0, port: output.port }, EgressNub { targets, phantom: PhantomData }, channel_id);

        Stream::new(
            output,
            registrar,
            scope.parent.clone()
        )
    }
}


struct IngressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Data> {
    targets: Counter<TInner, TData, Tee<TInner, TData>>,
    phantom: ::std::marker::PhantomData<TOuter>,
}

impl<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Data> Push<Bundle<TOuter, TData>> for IngressNub<TOuter, TInner, TData> {
    fn push(&mut self, message: &mut Option<Bundle<TOuter, TData>>) {
        if let Some(message) = message {
            let outer_message = message.as_mut();
            let data = ::std::mem::replace(&mut outer_message.data, Vec::new());
            let mut inner_message = Some(Bundle::from_typed(Message::new(TInner::to_inner(outer_message.time.clone()), data, 0, 0)));
            self.targets.push(&mut inner_message);
            if let Some(inner_message) = inner_message {
                if let Some(inner_message) = inner_message.if_typed() {
                    outer_message.data = inner_message.data;
                }
            }
        }
        else { self.targets.done(); }
    }
}


struct EgressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Data> {
    targets: Tee<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Push<Bundle<TInner, TData>> for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Data {
    fn push(&mut self, message: &mut Option<Bundle<TInner, TData>>) {
        if let Some(message) = message {
            let inner_message = message.as_mut();
            let data = ::std::mem::replace(&mut inner_message.data, Vec::new());
            let mut outer_message = Some(Bundle::from_typed(Message::new(inner_message.time.clone().to_outer(), data, 0, 0)));
            self.targets.push(&mut outer_message);
            if let Some(outer_message) = outer_message {
                if let Some(outer_message) = outer_message.if_typed() {
                    inner_message.data = outer_message.data;
                }
            }
        }
        else { self.targets.done(); }
    }
}
