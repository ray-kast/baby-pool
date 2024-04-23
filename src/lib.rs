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

pub mod graph;
pub mod nonblock;
pub mod blocking;
mod executor;

use std::{
    future::Future,
    panic::{RefUnwindSafe, UnwindSafe},
};

pub use graph::ExecutorBuilderExt;

pub mod prelude {
    //! Common traits

    pub use super::{
        graph::SchedulerCore, ExecutorAsync, ExecutorBuilder, ExecutorBuilderExt, ExecutorCore,
        ExecutorHandle, ExecutorSync,
    };
}

/// Builder type for an executor
pub trait ExecutorBuilder<J: UnwindSafe, E: ExecutorCore<J>, F> {
    /// The error type for [`Self::build`]
    type Error: std::error::Error;

    /// Consume the builder, producing an executor
    ///
    /// # Errors
    /// This function will fail if an error occurred previously while
    /// configuring the builder, or if an error occurred while initializing
    /// the executor.
    fn build(
        self,
        work: impl Fn(J, E::Handle<'_>) -> F + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<E, Self::Error>;
}

/// The smallest possible abstraction over an [`Executor`] and its associated
/// [`Handle`](Executor::Handle)
pub trait ExecutorHandle<J> {
    /// Push a new job onto the executor for running as soon as possible
    fn push(&self, job: J);
}

// TODO: assert unwind safe in the right places

/// Abstraction over a thread pool that executes jobs in a dependency-free queue
pub trait ExecutorCore<J: UnwindSafe>: ExecutorHandle<J> + Sized {
    /// The handle into this executor exposed to running jobs
    type Handle<'a>: ExecutorHandle<J> + Copy + UnwindSafe + RefUnwindSafe + 'a;
}

pub trait ExecutorSync<J: UnwindSafe>: ExecutorCore<J> {
    /// Disable pushing new jobs and wait for all pending work to complete,
    /// including jobs queued by currently-running jobs
    fn join(self);

    /// Disable pushing new jobs and wait for all currently-running jobs to
    /// finish before dropping the rest
    fn abort(self);
}

pub trait ExecutorAsync<J: UnwindSafe>: ExecutorCore<J> {
    /// Disable pushing new jobs and wait for all pending work to complete,
    /// including jobs queued by currently-running jobs
    fn join(self) -> impl Future<Output = ()> + Send;

    /// Disable pushing new jobs and wait for all currently-running jobs to
    /// finish before dropping the rest
    fn abort(self) -> impl Future<Output = ()> + Send;
}
