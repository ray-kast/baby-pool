//! A concurrent implementation of toposort for handling task dependencies.
//!
//! This module is designed to parallelize operations on a dependency graph
//! where some tasks must be delayed until all dependencies are met, such as in
//! the following case:
//!
//! ```
//! # use topograph::{prelude::*, threaded, graph};
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
//! let pool = threaded::Builder::default().build_graph(move |job, handle| {
//!     match job {
//!         Job::Foo => {
//!             let mut deps = handle.create_node(Job::Assert, 2);
//!
//!             handle.push_dependency(Job::Bar, Some(deps.take()));
//!             handle.push_dependency(Job::Baz, Some(deps.take()));
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
    panic::{AssertUnwindSafe, RefUnwindSafe, UnwindSafe},
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::prelude::*;

/// The core interface for scheduling tasks with dependencies
pub trait SchedulerCore<'a, J> {
    /// Create a pending job
    ///
    /// # Panics
    /// This function panics if `dependencies` is an invalid value of `0` or
    /// `usize::MAX`
    fn create_node(&self, payload: J, dependencies: usize) -> DependencyBag<J> {
        self.create_node_or_run(payload, dependencies).unwrap()
    }

    /// Create a pending job or run it if it has no dependencies
    ///
    /// # Panics
    /// This function panics if `dependencies` is equal to `usize::MAX`, which
    /// is a reserved number
    fn create_node_or_run(&self, payload: J, dependencies: usize) -> Option<DependencyBag<J>>;

    /// Add a job to the graph, as well as the jobs that should run after it
    fn push_dependency(&self, payload: J, dependents: impl IntoIterator<Item = Arc<Node<J>>>);
}

/// A reference to a job that may be pending
#[derive(Debug)]
pub struct Node<J> {
    payload: ManuallyDrop<UnsafeCell<MaybeUninit<J>>>,
    dependencies: AtomicUsize,
}

// Rationale: J can only be accessed by the last dependency as it polls this
// node, or by the Drop impl.
unsafe impl<J> Sync for Node<J> {}

/// Helper struct for parceling out node dependencies
///
/// # Panics
/// This struct panics on drop if not all dependencies are used, as this results
/// in a node that cannot run.
#[derive(Debug)]
pub struct DependencyBag<J> {
    node: Option<Arc<Node<J>>>,
    remaining: usize,
}

type Dependents<J> = Box<[Arc<Node<J>>]>;

/// A job payload and associated dependency information
#[derive(Debug)]
pub struct Job<J> {
    payload: J,
    dependents: AssertUnwindSafe<Option<Dependents<J>>>,
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

impl<J> Drop for Node<J> {
    fn drop(&mut self) {
        match self.dependencies.swap(0, Ordering::SeqCst) {
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

impl<J> DependencyBag<J> {
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
                    dependencies: AtomicUsize::new(dependencies),
                });

                Some(DependencyBag {
                    node: Some(node),
                    remaining: dependencies,
                })
            },
        }
    }

    /// Request a single dependency.
    ///
    /// # Panics
    /// This method panics if no more dependency handles are available.
    #[inline]
    pub fn take(&mut self) -> Arc<Node<J>> { self.try_take().unwrap() }

    /// Request a single dependency, returning `None` if no more are available.
    #[inline]
    pub fn try_take(&mut self) -> Option<Arc<Node<J>>> {
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

        if node.is_none() {
            unreachable!();
        }
        node
    }
}

impl<J> Drop for DependencyBag<J> {
    fn drop(&mut self) {
        assert!(
            self.remaining == 0 || self.node.is_none(),
            "Failed to exhaust dependency bag!"
        );
    }
}

impl<J> From<J> for Job<J> {
    fn from(payload: J) -> Self {
        Self {
            payload,
            dependents: AssertUnwindSafe(None),
        }
    }
}

impl<'a, J, H: ExecutorHandle<'a, Job<J>>> SchedulerCore<'a, J> for Handle<H> {
    fn create_node_or_run(&self, payload: J, dependencies: usize) -> Option<DependencyBag<J>> {
        DependencyBag::create_or_run(payload, dependencies, |j| self.0.push(j.into()))
    }

    fn push_dependency(&self, payload: J, dependents: impl IntoIterator<Item = Arc<Node<J>>>) {
        self.0.push(Job {
            payload,
            dependents: AssertUnwindSafe(Some(
                dependents
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )),
        });
    }
}

impl<'a, J, H: ExecutorHandle<'a, Job<J>>> ExecutorHandle<'a, J> for Handle<H> {
    fn push(&self, job: J) { self.0.push(job.into()); }
}

impl<J: UnwindSafe, E: Executor<Job<J>>> Scheduler<J, E>
where for<'a> E::Handle<'a>: Clone
{
    /// Construct a new graph scheduler
    fn new<B: ExecutorBuilder<Job<J>, E>>(
        b: B,
        f: impl Fn(J, Handle<E::Handle<'_>>) -> Result<(), ()> + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<Self, B::Error> {
        let executor = b.build(
            move |Job {
                      payload,
                      dependents,
                  },
                  handle| match f(payload, Handle(handle)) {
                Ok(()) => {
                    if let Some(dependents) = dependents.0 {
                        for dep in Vec::from(dependents) {
                            match dep.dependencies.fetch_sub(1, Ordering::SeqCst) {
                                1 => {
                                    let job = {
                                        let mut taken = MaybeUninit::zeroed();

                                        unsafe {
                                            ptr::swap(dep.payload.get(), &mut taken);
                                            taken.assume_init()
                                        }
                                    };

                                    // TODO: this means we don't handle transitive dependencies
                                    handle.push(job.into());
                                },
                                0 | usize::MAX => unreachable!(),
                                _ => (),
                            }
                        }
                    }
                },
                Err(()) => (),
            },
        )?;

        Ok(Self {
            executor,
            _m: PhantomData::default(),
        })
    }
}

/// Adds the [`build_graph`](ExecutorBuilderExt::build_graph) method to
/// [`ExecutorBuilder`]
pub trait ExecutorBuilderExt<J: UnwindSafe, E: Executor<Job<J>>>:
    Sized + ExecutorBuilder<Job<J>, E>
{
    /// Construct a new graph scheduler using this builder's executor type
    ///
    /// # Errors
    /// This method fails if the underlying executor fails to build.
    fn build_graph(
        self,
        work: impl Fn(J, Handle<E::Handle<'_>>) -> Result<(), ()>
        + Send
        + Clone
        + RefUnwindSafe
        + 'static,
    ) -> Result<Scheduler<J, E>, Self::Error>;
}

impl<J: UnwindSafe, E: Executor<Job<J>>, B: ExecutorBuilder<Job<J>, E> + Sized>
    ExecutorBuilderExt<J, E> for B
{
    fn build_graph(
        self,
        work: impl Fn(J, Handle<E::Handle<'_>>) -> Result<(), ()>
        + Send
        + Clone
        + RefUnwindSafe
        + 'static,
    ) -> Result<Scheduler<J, E>, Self::Error> {
        Scheduler::new(self, work)
    }
}

impl<J: UnwindSafe, E: Executor<Job<J>>> Executor<J> for Scheduler<J, E> {
    type Handle<'a> = Handle<E::Handle<'a>>;

    #[inline]
    fn push(&self, job: J) { self.executor.push(job.into()); }

    #[inline]
    fn join(self) { self.executor.join(); }

    #[inline]
    fn abort(self) { self.executor.abort(); }
}

impl<'a, J: UnwindSafe, E: Executor<Job<J>>> SchedulerCore<'a, J> for Scheduler<J, E> {
    fn create_node_or_run(&self, payload: J, dependencies: usize) -> Option<DependencyBag<J>> {
        DependencyBag::create_or_run(payload, dependencies, |j| self.executor.push(j.into()))
    }

    fn push_dependency(&self, payload: J, dependents: impl IntoIterator<Item = Arc<Node<J>>>) {
        self.executor.push(Job {
            payload,
            dependents: AssertUnwindSafe(Some(
                dependents
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )),
        });
    }
}
