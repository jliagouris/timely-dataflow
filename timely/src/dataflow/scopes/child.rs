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
use crate::state::{StateBackend, StateBackendInfo, StateHandle};

use super::{ScopeParent, Scope};

/// Type alias for iterative child scope.
pub type Iterative<'a, G, T, S> = Child<'a, G, Product<<G as ScopeParent>::Timestamp, T>, S>;

/// A `Child` wraps a `Subgraph` and a parent `G: Scope`. It manages the addition
/// of `Operate`s to a subgraph, and the connection of edges between them.
pub struct Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend,
{
    /// The subgraph under assembly.
    pub subgraph: &'a RefCell<SubgraphBuilder<G::Timestamp, T>>,
    /// A copy of the child's parent scope.
    pub parent:   G,
    /// The log writer for this scope.
    logging:  Option<Logger>,
    /// The state backend for this code.
    state_backend: Rc<S>,
    /// The information required for spawning state backends
    state_backend_info: StateBackendInfo,
}

impl<'a, G, T, S> Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend
{

    /// New
    pub fn new(subgraph: &'a RefCell<SubgraphBuilder<G::Timestamp, T>>, parent: G, logging: Option<Logger>, state_directory: &str) -> Self {
        // TODO: check sizing
        /*
        let faster_kv = Rc::new(FasterKv::new(
            1 << 15,
            4294967296 / 2, //2GB
            directory.path().to_str().unwrap().to_owned(),
        ).unwrap());
        */
        let faster_kv = Rc::new(FasterKv::new(
            1 << 15,
            4294967296, //4GB
            state_directory.to_string(),
        ).unwrap());
        let state_backend_info = StateBackendInfo::new(&faster_kv);
        faster_kv.start_session();
        Child {
            subgraph,
            parent,
            logging,
            state_backend: Rc::new(S::new(&state_backend_info)),
            state_backend_info,
        }

    }

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
    S: StateBackend
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
    S: StateBackend
{
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.parent.activations()
    }
}

impl<'a, G, T, S> ScopeParent for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend
{
    type Timestamp = T;
}

impl<'a, G, T, S> Scope for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend
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
                state_backend: self.state_backend.clone(),
                state_backend_info: self.state_backend_info.clone(),
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(self);

        self.add_operator_with_index(Box::new(subscope), index);

        result
    }

    fn get_state_handle(&self) -> StateHandle<S> {
        let name = [&self.index().to_string(), "."].join("");
        StateHandle::new(self.state_backend.clone(), &name)
    }

    fn get_state_backend_info(&self) -> &StateBackendInfo {
        &self.state_backend_info
    }
}

use crate::communication::Message;
use faster_rs::FasterKv;

impl<'a, G, T, S> Clone for Child<'a, G, T, S>
where
    G: ScopeParent,
    T: Timestamp+Refines<G::Timestamp>,
    S: StateBackend
{
    fn clone(&self) -> Self {
        Child {
            subgraph: self.subgraph,
            parent: self.parent.clone(),
            logging: self.logging.clone(),
            state_backend: self.state_backend.clone(),
            state_backend_info: self.state_backend_info.clone(),
        }
    }
}
