//! A child dataflow scope, used to build nested dataflow scopes.

use std::rc::Rc;
use std::cell::RefCell;

use crate::communication::{Data, Push, Pull};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::scheduling::Scheduler;
use crate::scheduling::activate::Activations;
use crate::progress::{Timestamp, Operate, SubgraphBuilder};
use crate::progress::{Source, Target};
use crate::progress::timestamp::Refines;
use crate::order::Product;
use crate::logging::TimelyLogger as Logger;
use crate::worker::AsWorker;
use crate::state::{StateBackend, StateHandle};
use faster_rs::FasterKv;
use std::marker::PhantomData;

use super::{ScopeParent, Scope};

/// Type alias for iterative child scope.
pub type Iterative<'a, G, T, S> = Child<'a, G, Product<<G as ScopeParent>::Timestamp, T>, S>;

/// A `Child` wraps a `Subgraph` and a parent `G: Scope`. It manages the addition
/// of `Operate`s to a subgraph, and the connection of edges between them.
pub struct Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend<'a>,
{
    /// The subgraph under assembly.
    pub subgraph: &'a RefCell<SubgraphBuilder<G::Timestamp, T>>,
    /// A copy of the child's parent scope.
    pub parent:   G,
    /// The log writer for this scope.
    pub logging:  Option<Logger>,
    pub faster: &'a FasterKv,
    pub monotonic_serial_number: Rc<RefCell<u64>>,
    phantom: PhantomData<S>,
    /*
    /// The state backend for this code.
    pub state_backend: Rc<RefCell<S>>,
    /// The information required for spawning state backends
    pub state_backend_info: StateBackendInfo<'static>,
    */
}

impl<'a, G, T, S> Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend<'a>,
{
    /// This worker's unique identifier.
    ///
    /// Ranges from `0` to `self.peers() - 1`.
    pub fn index(&self) -> usize { self.parent.index() }
    /// The total number of workers in the computation.
    pub fn peers(&self) -> usize { self.parent.peers() }
}

impl<'a, G, T, S> AsWorker for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend<'a>
{
    fn index(&self) -> usize { self.parent.index() }
    fn peers(&self) -> usize { self.parent.peers() }
    fn allocate<D: Data>(&mut self, identifier: usize, address: &[usize]) -> (Vec<Box<Push<Message<D>>>>, Box<Pull<Message<D>>>) {
        self.parent.allocate(identifier, address)
    }
    fn pipeline<D: 'static>(&mut self, identifier: usize, address: &[usize]) -> (ThreadPusher<Message<D>>, ThreadPuller<Message<D>>) {
        self.parent.pipeline(identifier, address)
    }
    fn new_identifier(&mut self) -> usize {
        self.parent.new_identifier()
    }
    fn log_register(&self) -> ::std::cell::RefMut<crate::logging_core::Registry<crate::logging::WorkerIdentifier>> {
        self.parent.log_register()
    }
}

impl<'a, G, T, S> Scheduler for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend<'a>,
{
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.parent.activations()
    }
}

impl<'a, G, T, S> ScopeParent for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend<'a>,
{
    type Timestamp = T;
}

impl<'a, G, T, S> Scope<'a> for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend<'a>,
{
    type StateBackend = S;

    fn name(&self) -> String { self.subgraph.borrow().name.clone() }
    fn addr(&self) -> Vec<usize> { self.subgraph.borrow().path.clone() }
    fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    fn add_operator_with_indices(&mut self, operator: Box<Operate<Self::Timestamp>>, local: usize, global: usize) {
        self.subgraph.borrow_mut().add_child(operator, local, global);
    }

    fn allocate_operator_index(&mut self) -> usize {
        self.subgraph.borrow_mut().allocate_child_id()
    }

    #[inline]
    fn scoped<T2, R, F>(&mut self, name: &str, func: F) -> R
    where
        T2: Timestamp+Refines<T>,
        F: FnOnce(&mut Child<Self, T2, S>) -> R,
    {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let path = self.subgraph.borrow().path.clone();

        let subscope = RefCell::new(SubgraphBuilder::new_from(index, path, self.logging().clone(), name));
        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.logging.clone(),
                faster: self.faster,
                monotonic_serial_number: Rc::clone(&self.monontonic_serial_number),
                phantom: PhantomData
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(self);

        self.add_operator_with_index(Box::new(subscope), index);

        result
    }

    fn get_state_handle(&self) -> StateHandle<'a, Self::StateBackend> {
        let name = [&self.index().to_string(), ".".to_string()].join("");
        StateHandle::new(S::new(self.faster, Rc::clone(&self.monotonic_serial_number)), &name)
    }

}

use crate::communication::Message;

impl<'a, G, T, S> Clone for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend<'a> {
    fn clone(&self) -> Self {
        Child {
            subgraph: self.subgraph,
            parent: self.parent.clone(),
            logging: self.logging.clone(),
            faster: self.faster,
            monotonic_serial_number: Rc::clone(&self.monontonic_serial_number),
            phantom: PhantomData,
        }
    }
}
