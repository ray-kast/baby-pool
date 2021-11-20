#![deny(
    clippy::suspicious,
    clippy::style,
    missing_debug_implementations,
    missing_copy_implementations,
    rustdoc::broken_intra_doc_links
)]
#![warn(clippy::pedantic, clippy::cargo, missing_docs)]
#![feature(generic_associated_types)]

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

pub mod graph;
pub mod threaded;

use std::panic::{RefUnwindSafe, UnwindSafe};

pub use graph::ExecutorBuilderExt;

pub mod prelude {
    //! Common traits

    pub use super::{
        graph::SchedulerCore, Executor, ExecutorBuilder, ExecutorBuilderExt, ExecutorHandle,
    };
}

/// Builder type for an executor
pub trait ExecutorBuilder<J: UnwindSafe, E: Executor<J>> {
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
        work: impl Fn(J, E::Handle<'_>) + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<E, Self::Error>;
}

/// Abstraction over a handle into an [`Executor`] accessible to concurrent jobs
pub trait ExecutorHandle<'a, J>: Copy + UnwindSafe + RefUnwindSafe + 'a {
    /// Push a new job onto the executor for running as soon as possible
    fn push(&self, job: J);
}

/// Abstraction over a thread pool that executes jobs in a dependency-free queue
pub trait Executor<J: UnwindSafe>: Sized {
    /// The handle into this executor exposed to running jobs
    type Handle<'a>: ExecutorHandle<'a, J>;

    /// Push a new job onto the executor for running as soon as possible
    fn push(&self, job: J);

    /// Disable pushing new jobs and wait for all pending work to complete,
    /// including jobs queued by currently-running jobs
    fn join(self);

    /// Disable pushing new jobs and wait for all currently-running jobs to
    /// finish before dropping the rest
    fn abort(self);
}
