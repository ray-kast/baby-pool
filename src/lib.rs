#![deny(
    clippy::disallowed_methods,
    clippy::suspicious,
    clippy::style,
    clippy::clone_on_ref_ptr,
    missing_debug_implementations,
    missing_copy_implementations
)]
#![warn(clippy::pedantic, missing_docs)]
#![allow(clippy::module_name_repetitions)]
// TODO: document everything
#![allow(
    missing_docs,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::missing_safety_doc
)]

//! A tiny library for doing basic synchronous thread scheduling.
//!
//! This crate contains two main components, each with their own (slightly
//! different) use cases:
//!  - [`threaded`] is a simple FIFO thread pool for basic work queue operations
//!  - [`graph`] is a topological sort implementation for handling more complex
//!    cases with inter-task dependencies
//!
//! The documentation for each of the above modules contains example code for
//! each use case.

// TODO: make sync and async dependencies optional

use std::future::Future;

pub mod executor;
pub mod graph;
mod nonblock;

pub mod prelude {
    //! Common traits

    pub use super::{
        graph::{ExecutorBuilderExt, SchedulerCore},
        ExecutorBuilderAsync, ExecutorBuilderCore, ExecutorBuilderSync, ExecutorCore,
        ExecutorHandle,
    };
}

// TODO: hack to workaround the lack of AsyncFn
pub trait AsyncHandler<J, H> {
    type Output;

    fn handle(&self, job: J, handle: H) -> impl Future<Output = Self::Output> + Send;
}

impl<J, H, T: Future + Send, F: Fn(J, H) -> T> AsyncHandler<J, H> for F {
    type Output = T::Output;

    fn handle(&self, job: J, handle: H) -> impl Future<Output = Self::Output> + Send {
        self(job, handle)
    }
}

// TODO: clean up the API for this
pub trait ExecutorBuilderCore<J> {
    /// The error type for [`Self::build`]
    type Error: std::error::Error;

    type Executor: ExecutorCore<J>;
}

/// Builder type for an executor
pub trait ExecutorBuilder<J, F>: ExecutorBuilderCore<J> {
    /// Consume the builder, producing an executor
    ///
    /// # Errors
    /// This function will fail if an error occurred previously while
    /// configuring the builder, or if an error occurred while initializing
    /// the executor.
    fn build(self, work: F) -> Result<Self::Executor, Self::Error>;
}

// TODO: ExecutorBuilderSync and ExecutorBuilderAsync are hacks to work around
//       build_graph being unable to name the type parameter F in ExecutorBuilder

pub trait ExecutorBuilderSync<J>: ExecutorBuilderCore<J> {
    fn build<F: Fn(J, <Self::Executor as ExecutorCore<J>>::Handle<'_>) + Clone + Send + 'static>(
        self,
        work: F,
    ) -> Result<Self::Executor, Self::Error>;
}

pub trait ExecutorBuilderAsync<J>: ExecutorBuilderCore<J> {
    fn build_async<
        F: for<'h> AsyncHandler<J, <Self::Executor as ExecutorCore<J>>::Handle<'h>, Output = ()>
            + Clone
            + Send
            + 'static,
    >(
        self,
        work: F,
    ) -> Result<Self::Executor, Self::Error>;
}

/// The smallest possible abstraction over an [`Executor`] and its associated
/// [`Handle`](Executor::Handle)
pub trait ExecutorHandle<J> {
    /// Push a new job onto the executor for running as soon as possible
    fn push(&self, job: J);
}

// TODO: assert unwind safe in the right places
// TODO: check usages of atomic ordering

/// Abstraction over a thread pool that executes jobs in a dependency-free queue
pub trait ExecutorCore<J>: ExecutorHandle<J> + Sized {
    /// The handle into this executor exposed to running jobs
    type Handle<'a>: ExecutorHandle<J> + Copy + 'a;
}
