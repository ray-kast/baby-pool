//! A concurrent implementation of toposort for handling task dependencies.
//!
//! This module is designed to parallelize operations on a dependency graph
//! where some tasks must be delayed until all dependencies are met, such as in
//! the following case:
//!
//! ```
//! # use topograph::{prelude::*, executor, graph};
//! # use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
//! enum Job {
//!     Foo,
//!     Bar,
//!     Baz,
//!     Assert,
//! }
//!
//! let data = Arc::new(AtomicU32::new(0));
//! # let data_1 = data.clone();
//!
//! let pool = executor::Builder::new(executor::Blocking).build_graph(move |job, handle| {
//!     match job {
//!         Job::Foo => {
//!             let mut deps = handle.create_node(Job::Assert, 2);
//!
//!             handle.push_dependency(Job::Bar, Some(deps.get_in_edge()));
//!             handle.push_dependency(Job::Baz, Some(deps.get_in_edge()));
//!         },
//!         Job::Bar => {
//!             data.fetch_add(1, Ordering::SeqCst);
//!         },
//!         Job::Baz => {
//!             data.fetch_add(2, Ordering::SeqCst);
//!         },
//!         Job::Assert => {
//!             assert_eq!(data.swap(1337, Ordering::SeqCst), 3);
//!         },
//!     }
//!
//!     Ok(())
//! }).unwrap();
//!
//! pool.push(Job::Foo);
//! pool.join();
//! # let data = data_1;
//!
//! assert_eq!(data.load(Ordering::SeqCst), 1337);
//! ```
//!
//! The above code does not panic because the scheduler ensures `Assert` is not
//! processed until `Bar` and `Baz` complete.
//!
//! Additionally, since dependent tasks should only run if all dependencies
//! succeed, this module alters the worker function to allow returning a simple
//! `Result` value.

use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem,
    mem::{ManuallyDrop, MaybeUninit},
    ops, ptr,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use thiserror::Error;

use crate::{
    executor::{AsyncExecutor, Blocking, Executor, Nonblock},
    prelude::*,
};

/// The core interface for scheduling tasks with dependencies
pub trait SchedulerCore<J>: ExecutorHandle<J> {
    /// Create a pending job
    ///
    /// # Panics
    /// This function panics if `dependencies` is an invalid value of `0` or
    /// `usize::MAX`
    fn create_node(&self, payload: J, dependencies: usize) -> NodeBuilder<J> {
        self.create_node_or_run(payload, dependencies).unwrap()
    }

    /// Create a pending job or run it if it has no dependencies
    ///
    /// # Panics
    /// This function panics if `dependencies` is equal to `usize::MAX`, which
    /// is a reserved number
    fn create_node_or_run(&self, payload: J, dependencies: usize) -> Option<NodeBuilder<J>>;

    /// Add a job to the graph, as well as an optional reference to the edges
    /// that should run after it.
    ///
    /// Other references to the value passed for dependents may be later used
    /// for lazily scheduling dependent jobs.  If no reference is passed, this
    /// function must behave identically to [`ExecutorHandle::push`]
    fn push_with_dependents(&self, payload: J, dependents: OptRcDependents<J>);

    /// Add a job to the graph, as well as the jobs that should run after it.
    ///
    /// Returns a handle to allow lazily scheduling dependent jobs.
    fn push_dependency(
        &self,
        payload: J,
        dependents: impl IntoIterator<Item = Edge<J>>,
    ) -> Arc<Dependents<J>> {
        let deps = Dependents::new(dependents.into_iter().collect());

        self.push_with_dependents(payload, Some(Arc::clone(&deps)));

        deps
    }
}

// manually drop, and unsafe cell, and maybe uninit, oh my!
type NodePayload<T> = ManuallyDrop<UnsafeCell<MaybeUninit<T>>>;

#[derive(Debug)]
struct Node<J> {
    payload: NodePayload<J>,
    dependents: AtomicPtr<Dependents<J>>,
    dependencies: AtomicUsize,
}

/// A handle to a pending job.  When "decremented," the number of unsatisfied
/// dependencies this edge points to is reduced by one, and the job is run once
/// all dependencies are marked as satisfied.
#[derive(Debug)]
#[repr(transparent)]
pub struct Edge<J> {
    to: Arc<Node<J>>,
}

/// An error for operations performed on a [`NodeBuilder`] after the last
/// reference to the node has been assigned to an inbound edge.
///
/// The reason for this error is that a node cannot be accessed once all inbound
/// (dependency) edges are exposed as at this point it becomes possible for the
/// node to be marked as ready to execute and for it to be destructured into a
/// queue job.
#[derive(Debug, Clone, Copy, Error)]
#[error("The node associated with this builder can no longer be accessed")]
pub struct NodeDispatched;

/// Helper struct for parceling out node dependencies
///
/// # Panics
/// This struct panics on drop if not all dependencies are used, as this results
/// in a node that cannot run.
#[derive(Debug)]
pub struct NodeBuilder<J> {
    node: Option<Arc<Node<J>>>,
    remaining: usize,
}

/// A reference to the jobs dependent on a queued job.
#[derive(Debug)]
pub struct Dependents<J>(RwLock<Option<Vec<Edge<J>>>>);

#[derive(Debug)]
enum AdoptState<J> {
    Orphan(Vec<Edge<J>>),
    Adopted(Arc<Dependents<J>>),
    Abandoned,
    Completed,
    Poisoned,
}

/// An error for operations performed on an [`AdoptableDependents`] list in an
/// invalid state.
#[derive(Debug, Clone, Copy, Error)]
#[error("Adoptable dependents have already been adopted or abandoned")]
pub struct BadAdoptState;

/// Helper type for tracking dependents of a job completely in parallel to its
/// discovery and enqueuing process.
///
/// This struct can handle tracking dependencies for:
///  - Jobs that do not exist in memory yet
///  - Jobs that have been discovered and that may or may not have completed
///  - Jobs that are known never to be created
///     - ...and which can be considered as already completed
///     - ...and which should be treated as never finishing
#[derive(Debug)]
pub struct AdoptableDependents<J>(AdoptState<J>);

/// A wrapper type for [`AdoptableDependents`] providing `Clone`, `Send`, and
/// `Sync` impls
#[derive(Debug)]
#[repr(transparent)]
pub struct RcAdoptableDependents<J>(Arc<Mutex<AdoptableDependents<J>>>);

type OptRcDependents<J> = Option<Arc<Dependents<J>>>;

/// A job payload and associated dependency information
#[derive(Debug)]
pub struct Job<J> {
    payload: J,
    dependents: OptRcDependents<J>,
}

/// A handle into the graph scheduler for running jobs
#[derive(Debug, Clone, Copy)]
pub struct Handle<H>(H);

/// Job scheduler using topological sort to manage dependencies
#[derive(Debug)]
pub struct Scheduler<J, E> {
    executor: E,
    _m: PhantomData<J>,
}

// Rationale: J can only be accessed by the last dependency as it polls this
// node, or by the Drop impl.
unsafe impl<J> Sync for Node<J> {}

impl<J> Node<J> {
    fn decrement<H: SchedulerCore<J>>(&self, handle: &H) {
        match self.dependencies.fetch_sub(1, Ordering::SeqCst) {
            1 => {
                let job = {
                    let mut taken = MaybeUninit::zeroed();

                    unsafe {
                        ptr::swap(self.payload.get(), &mut taken);
                        taken.assume_init()
                    }
                };

                let dependents = {
                    let ptr = self.dependents.swap(ptr::null_mut(), Ordering::SeqCst);

                    if ptr.is_null() {
                        None
                    } else {
                        Some(unsafe { Arc::from_raw(ptr) })
                    }
                };

                handle.push_with_dependents(job, dependents);
            },
            0 | usize::MAX => unreachable!(),
            _ => (),
        }
    }

    /// Returns `dependents` on error
    fn set_dependents(&self, dependents: Arc<Dependents<J>>) -> Result<(), Arc<Dependents<J>>> {
        let ptr = Arc::into_raw(dependents);

        self.dependents
            .compare_exchange(
                ptr::null_mut(),
                ptr.cast_mut(),
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .map(|_| ())
            .map_err(|_| unsafe { Arc::from_raw(ptr) })
    }
}

impl<J> Drop for Node<J> {
    fn drop(&mut self) {
        match mem::replace(self.dependencies.get_mut(), 0) {
            0 => (),
            usize::MAX => unreachable!(),
            _ => unsafe {
                mem::drop(
                    ManuallyDrop::take(&mut self.payload)
                        .into_inner()
                        .assume_init(),
                );
            },
        }
    }
}

impl<J> Edge<J> {
    fn new(to: Arc<Node<J>>) -> Self { Self { to } }
}

impl<J> NodeBuilder<J> {
    fn create_or_run(payload: J, dependencies: usize, run: impl FnOnce(J)) -> Option<Self> {
        match dependencies {
            0 => {
                run(payload);

                None
            },
            usize::MAX => panic!("Invalid number of dependencies! (usize::MAX is reserved)"),
            _ => {
                let node = Arc::new(Node {
                    payload: ManuallyDrop::new(UnsafeCell::new(MaybeUninit::new(payload))),
                    dependents: AtomicPtr::new(ptr::null_mut()),
                    dependencies: AtomicUsize::new(dependencies),
                });

                Some(NodeBuilder {
                    node: Some(node),
                    remaining: dependencies,
                })
            },
        }
    }

    /// Request a single inbound edge (dependency).
    ///
    /// # Panics
    /// This method panics if no more dependency handles are available.
    #[inline]
    pub fn get_in_edge(&mut self) -> Edge<J> { self.try_get_in_edge().unwrap() }

    /// Request a single inbound edge (dependency), returning `None` if no more
    /// are available.
    pub fn try_get_in_edge(&mut self) -> Option<Edge<J>> {
        if self.remaining > 0 && self.node.is_none() {
            unreachable!();
        }

        let node = match self.remaining {
            0 => None,
            1 => {
                self.remaining = 0;
                self.node.take()
            },
            _ => {
                self.remaining -= 1;
                self.node.clone()
            },
        };

        node.map(Edge::new)
    }

    /// Add a list of dependents to this node.
    ///
    /// # Errors
    /// This function returns an error if the node is no longer available to be
    /// accessed safely or if a dependent list has already been assigned to the
    /// node.
    pub fn set_dependents(
        &mut self, // NOTE: this mut is IMPORTANT!
        dependents: Arc<Dependents<J>>,
    ) -> Result<(), Arc<Dependents<J>>> {
        let Some(node) = self.node.as_ref() else {
            return Err(dependents);
        };

        debug_assert!(self.remaining > 0);
        debug_assert!(node.dependencies.load(Ordering::SeqCst) >= self.remaining);

        node.set_dependents(dependents)?;

        Ok(())
    }
}

impl<J> Drop for NodeBuilder<J> {
    fn drop(&mut self) {
        assert!(
            self.remaining == 0 || self.node.is_none(),
            "Failed to exhaust dependency bag!"
        );
    }
}

impl<J> Dependents<J> {
    /// Construct a new dependent list from its inner vector
    #[must_use]
    pub fn new(dependents: Vec<Edge<J>>) -> Arc<Self> {
        Arc::new(Self(RwLock::new(Some(dependents))))
    }

    /// Push a new dependent job into this dependents list.
    ///
    /// If the job associated with this list has already run, the job will be
    /// enqueued immediately.
    pub fn push<H: SchedulerCore<J>>(&self, handle: &H, dependent: Edge<J>) {
        let this = self.0.upgradable_read();

        if this.is_some() {
            let mut this = RwLockUpgradableReadGuard::upgrade(this);
            let this = this.as_mut().unwrap_or_else(|| unreachable!());

            this.push(dependent);
        } else {
            dependent.to.decrement(handle);
        }
    }
}

impl<J> From<Edge<J>> for Arc<Dependents<J>> {
    #[inline]
    fn from(edge: Edge<J>) -> Self { Dependents::new(vec![edge]) }
}

impl<J> std::iter::FromIterator<Edge<J>> for Arc<Dependents<J>> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Edge<J>>>(it: I) -> Self {
        Dependents::new(it.into_iter().collect())
    }
}

impl<J> AdoptableDependents<J> {
    /// Construct a new `AdoptableDependents` in its initial "orphan" state.
    ///
    /// Calls to [`push`](Self::push) will hold pending jobs in a list to be
    /// processed with one of the state transition functions, such as
    /// [`adopt`](Self::adopt) or [`abandon`](Self::abandon).
    #[must_use]
    pub fn new() -> Self { Self(AdoptState::Orphan(vec![])) }

    /// Construct a new `AdoptableDependents` in an already-adopted state.
    ///
    /// Calls to [`push`](Self::push) will function identically to calling
    /// [`push`](Dependents::push) directly on the value provided for
    /// `dependents`.
    #[must_use]
    pub fn adopted(dependents: Arc<Dependents<J>>) -> Self { Self(AdoptState::Adopted(dependents)) }

    /// Construct a new `AdoptableDependencies` in an "abandoned" state.
    ///
    /// Calls to [`push`](Self::push) will function as if called on a job that
    /// failed, blocking any dependent jobs from running.
    #[must_use]
    pub fn abandoned() -> Self { Self(AdoptState::Abandoned) }

    /// Construct a new `AdoptableDependents` in a "completed" state.
    ///
    /// Calls to [`push`](Self::push) will function as if called on a job that
    /// has already completed, immediately decrementing the unsatisfied
    /// dependencies for the node.
    #[must_use]
    pub fn completed() -> Self { Self(AdoptState::Completed) }

    /// Wrap this `AdoptableDependents` in an `Arc<Mutex>` allowing for cloning
    /// and sending between threads.
    #[inline]
    #[must_use]
    pub fn rc(self) -> RcAdoptableDependents<J> {
        RcAdoptableDependents(Arc::new(Mutex::new(self)))
    }

    /// Add an edge to the list of dependents.
    ///
    /// The exact behavior varies depending on the underlying state of the
    /// `AdoptableDependents` instance:
    ///  - Orphaned instances will store the edge in a list of pending edges
    ///  - Adopted instances will pass the edge along to the adopted dependency
    ///    list, which will either queue it or decrement it immediately
    ///  - Completed instances will immediately decrement edges as if adopted by
    ///    a finished job
    ///  - Abandoned instances will do nothing, as if adopted by a failed job
    ///
    /// # Panics
    /// This function panics if the instance is found to be in a poisoned state.
    pub fn push<H: SchedulerCore<J>>(&mut self, handle: &H, dependent: Edge<J>) {
        match self.0 {
            AdoptState::Orphan(ref mut deps) => {
                deps.push(dependent);
            },
            AdoptState::Adopted(ref dependents) => dependents.push(handle, dependent),
            AdoptState::Abandoned => mem::drop(dependent),
            AdoptState::Completed => dependent.to.decrement(handle),
            AdoptState::Poisoned => panic!("AdoptableDependents was poisoned"),
        }
    }

    /// Attach an orphaned instance to a dependent list for an existing job.
    ///
    /// Any jobs stored internally will be forwarded to the adopted dependent
    /// list, as will any further calls to [`push`](Self::push).
    ///
    /// # Errors
    /// This function returns an error if this `AdoptableDependents` is in a
    /// state other than orphaned.
    ///
    /// # Panics
    /// This function panics if the instance is found to be in a poisoned state.
    pub fn adopt<H: SchedulerCore<J>>(
        &mut self,
        handle: &H,
        dependents: Arc<Dependents<J>>,
    ) -> Result<(), BadAdoptState> {
        match self.0 {
            AdoptState::Orphan(_) => (),
            AdoptState::Adopted(_) | AdoptState::Abandoned | AdoptState::Completed => {
                return Err(BadAdoptState);
            },
            AdoptState::Poisoned => panic!("AdoptableDependents was poisoned"),
        }

        if let AdoptState::Orphan(deps) = mem::replace(&mut self.0, AdoptState::Poisoned) {
            for dep in deps {
                dependents.push(handle, dep);
            }

            self.0 = AdoptState::Adopted(dependents);

            Ok(())
        } else {
            unreachable!()
        }
    }

    /// Abandon a non-adopted instance.
    ///
    /// Any stored jobs will be dropped and all future calls to
    /// [`push`](Self::push) will do nothing.  The value returned is true if
    /// this instance was not already abandoned.
    ///
    /// # Errors
    /// This function returns an error if this `AdoptableDependents` is not in
    /// the initial orphaned state or already abandoned.
    ///
    /// # Panics
    /// This function panics if the instance is found to be in a poisoned state.
    pub fn abandon(&mut self) -> Result<bool, BadAdoptState> {
        match self.0 {
            AdoptState::Orphan(_) => (),
            AdoptState::Adopted(_) | AdoptState::Completed => return Err(BadAdoptState),
            AdoptState::Abandoned => return Ok(false),
            AdoptState::Poisoned => panic!("AdoptableDependencies was poisoned"),
        }

        if let AdoptState::Orphan(jobs) = mem::replace(&mut self.0, AdoptState::Abandoned) {
            mem::drop(jobs);

            Ok(true)
        } else {
            unreachable!();
        }
    }

    /// Mark a non-adopted instance as completed.
    ///
    /// Any stored jobs will be decremented and all future calls to
    /// [`push`](Self::push) will behave as if passed to a completed job.
    /// The value returned is true if this instance was adopted or not
    /// already completed.
    ///
    /// # Errors
    /// This function returns an error if this `AdoptableDependents` is not in
    /// the initial orphaned state, already adopted, or already completed.
    ///
    /// # Panics
    /// This function panics if the instance is found to be in a poisoned state.
    pub fn complete<H: SchedulerCore<J>>(&mut self, handle: &H) -> Result<bool, BadAdoptState> {
        match self.0 {
            AdoptState::Orphan(_) => (),
            AdoptState::Adopted(_) | AdoptState::Completed => return Ok(false),
            AdoptState::Abandoned => return Err(BadAdoptState),
            AdoptState::Poisoned => panic!("AdoptableDependents was poisoned"),
        }

        if let AdoptState::Orphan(edges) = mem::replace(&mut self.0, AdoptState::Completed) {
            for edge in edges {
                edge.to.decrement(handle);
            }

            Ok(true)
        } else {
            unreachable!();
        }
    }
}

impl<J> Default for AdoptableDependents<J> {
    fn default() -> Self { Self::new() }
}

impl<J> ops::Deref for RcAdoptableDependents<J> {
    type Target = Mutex<AdoptableDependents<J>>;

    fn deref(&self) -> &Self::Target { self.0.as_ref() }
}

impl<J> Clone for RcAdoptableDependents<J> {
    fn clone(&self) -> Self { Self(Arc::clone(&self.0)) }
}

impl<J> From<J> for Job<J> {
    #[inline]
    fn from(payload: J) -> Self {
        Self {
            payload,
            dependents: None,
        }
    }
}

impl<J, H: ExecutorHandle<Job<J>>> SchedulerCore<J> for Handle<H> {
    fn create_node_or_run(&self, payload: J, dependencies: usize) -> Option<NodeBuilder<J>> {
        NodeBuilder::create_or_run(payload, dependencies, |j| self.0.push(j.into()))
    }

    #[inline]
    fn push_with_dependents(&self, payload: J, dependents: OptRcDependents<J>) {
        self.0.push(Job {
            payload,
            dependents,
        });
    }
}

impl<J, H: ExecutorHandle<Job<J>>> ExecutorHandle<J> for Handle<H> {
    #[inline]
    fn push(&self, job: J) { self.0.push(job.into()); }
}

impl<J, E: ExecutorCore<Job<J>>> Scheduler<J, E>
where for<'a> E::Handle<'a>: Clone
{
    /// Construct a new graph scheduler
    fn new<
        B: ExecutorBuilder<Job<J>, (), Executor = E>,
        F: Fn(J, Handle<E::Handle<'_>>) -> Result<(), ()> + Clone + Send + 'static,
    >(
        b: B,
        f: F,
    ) -> Result<Self, B::Error> {
        let executor = b.build(
            move |Job {
                      payload,
                      dependents,
                  },
                  handle| {
                let handle = Handle(handle);

                #[allow(clippy::single_match)]
                match f(payload, handle) {
                    Ok(()) => {
                        if let Some(dependents) = dependents {
                            for dep in mem::take(&mut *dependents.0.write()).into_iter().flatten() {
                                dep.to.decrement(&handle);
                            }
                        }
                    },
                    Err(()) => (),
                }
            },
        )?;

        Ok(Self {
            executor,
            _m: PhantomData,
        })
    }
}

impl<J, E> std::ops::Deref for Scheduler<J, E> {
    type Target = E;

    fn deref(&self) -> &E { &self.executor }
}

impl<J, E> std::ops::DerefMut for Scheduler<J, E> {
    fn deref_mut(&mut self) -> &mut E { &mut self.executor }
}

/// Adds the [`build_graph`](ExecutorBuilderExt::build_graph) method to
/// [`ExecutorBuilder`]
pub trait ExecutorBuilderExt<J>: Sized + ExecutorBuilder<Job<J>, ()> {
    // TODO: remove this impl Fn
    /// Construct a new graph scheduler using this builder's executor type
    ///
    /// # Errors
    /// This method fails if the underlying executor fails to build.
    fn build_graph(
        self,
        work: impl Fn(J, Handle<<Self::Executor as ExecutorCore<Job<J>>>::Handle<'_>>) -> Result<(), ()>
        + Send
        + Clone
        + 'static,
    ) -> Result<Scheduler<J, Self::Executor>, Self::Error>;
}

impl<J, B: ExecutorBuilder<Job<J>, ()> + Sized> ExecutorBuilderExt<J> for B {
    fn build_graph(
        self,
        work: impl Fn(J, Handle<<B::Executor as ExecutorCore<Job<J>>>::Handle<'_>>) -> Result<(), ()>
        + Send
        + Clone
        + 'static,
    ) -> Result<Scheduler<J, B::Executor>, Self::Error> {
        Scheduler::new(self, work)
    }
}

impl<J, E: ExecutorCore<Job<J>>> ExecutorHandle<J> for Scheduler<J, E> {
    #[inline]
    fn push(&self, job: J) { self.executor.push(job.into()); }
}

impl<J, E: ExecutorCore<Job<J>>> ExecutorCore<J> for Scheduler<J, E> {
    type Handle<'a> = Handle<E::Handle<'a>>;
}

impl<J: Send + 'static> Scheduler<J, Executor<J, Blocking>> {
    #[inline]
    pub fn join(self) { self.executor.join(); }

    #[inline]
    pub fn abort(self) { self.executor.abort(); }
}

impl<J: Send + 'static, E: AsyncExecutor> Scheduler<J, Executor<J, Nonblock<E>>> {
    #[inline]
    pub fn join_async(self) -> impl std::future::Future<Output = ()> + Send {
        self.executor.join_async()
    }

    #[inline]
    pub fn abort_async(self) -> impl std::future::Future<Output = ()> + Send {
        self.executor.abort_async()
    }
}

impl<J, E: ExecutorCore<Job<J>>> SchedulerCore<J> for Scheduler<J, E> {
    fn create_node_or_run(&self, payload: J, dependencies: usize) -> Option<NodeBuilder<J>> {
        NodeBuilder::create_or_run(payload, dependencies, |j| self.executor.push(j.into()))
    }

    #[inline]
    fn push_with_dependents(&self, payload: J, dependents: OptRcDependents<J>) {
        self.executor.push(Job {
            payload,
            dependents,
        });
    }
}
