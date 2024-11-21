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

//! A tiny library offering a concurrent work queue for synchronous or
//! asynchronous tasks, as well as an implementation of topological sort built
//! on top of it
//!
//! This crate contains two main components, each with their own (slightly
//! different) use cases:
//!  - [`executor`] contains the core logic for the work queue, as well as the
//!    necessary types and logic to initialize it
//!  - [`graph`] is a topological sort implementation for handling more complex
//!    cases with inter-task dependencies
//!
//! The documentation for each of the above modules contains example code for
//! each use case.

// TODO: make sync and async dependencies optional
// TODO: remove all references to the word "threads" in Executor
// TODO: make sure all the docs are updated to mention nonblocking support

use std::future::Future;

pub mod executor;
pub mod graph;
mod nonblock;

pub mod prelude {
    //! Common traits used when working with `topograph`

    pub use super::{
        graph::{ExecutorBuilderExt, SchedulerCore},
        ExecutorBuilderAsync, ExecutorBuilderCore, ExecutorBuilderSync, ExecutorCore,
        ExecutorHandle,
    };
}

// TODO: hack to workaround the lack of AsyncFn
/// A trait to work around the inability to express async lifetimes with [`Fn`]
///
/// Objects implementing [`AsyncHandler`] are expected to behave like the
/// (at the time of writing) unstable [`AsyncFn`](https://doc.rust-lang.org/nightly/unstable-book/library-features/async-fn-traits.html)
/// trait
pub trait AsyncHandler<J, H> {
    /// The result of executing this handler
    type Output;

    /// Return a future borrowing from this handler that, when polled,
    /// computes the result of executing this handler's logic
    fn handle(&self, job: J, handle: H) -> impl Future<Output = Self::Output> + Send;
}

impl<J, H, T: Future + Send, F: Fn(J, H) -> T> AsyncHandler<J, H> for F {
    type Output = T::Output;

    fn handle(&self, job: J, handle: H) -> impl Future<Output = Self::Output> + Send {
        self(job, handle)
    }
}

/// Trait describing common types for both [`ExecutorBuilderSync`] and
/// [`ExecutorBuilderAsync`]
pub trait ExecutorBuilderCore<J> {
    /// The error type for [`Self::build`](ExecutorBuilder::build)
    type Error: std::error::Error;

    /// The type of executor produced by this builder
    type Executor: ExecutorCore<J>;
}

/// Builder type for a work queue executor
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

/// Helper trait for describing an [`ExecutorBuilder`] that can accept any
/// [`Fn`] as a synchronous task
pub trait ExecutorBuilderSync<J>: ExecutorBuilderCore<J> {
    /// Consume the builder, producing an executor
    ///
    /// # Errors
    /// This function will fail if an error occurred previously while
    /// configuring the builder, or if an error occurred while initializing
    /// the executor.
    fn build<F: Fn(J, <Self::Executor as ExecutorCore<J>>::Handle<'_>) + Clone + Send + 'static>(
        self,
        work: F,
    ) -> Result<Self::Executor, Self::Error>;
}

/// Helper trait for describing an [`ExecutorBuilder`] that can accept any
/// [`AsyncHandler`] as a synchronous task
pub trait ExecutorBuilderAsync<J>: ExecutorBuilderCore<J> {
    /// Consume the builder, producing an executor
    ///
    /// # Errors
    /// This function will fail if an error occurred previously while
    /// configuring the builder, or if an error occurred while initializing
    /// the executor.
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

/// The smallest common abstraction over a work queue and associated
/// [task handle](ExecutorCore::Handle), providing solely a `push` function
pub trait ExecutorHandle<J> {
    /// Push a new job onto the executor for running as soon as possible
    fn push(&self, job: J);
}

// TODO: assert unwind safe in the right places
// TODO: check usages of atomic ordering

/// Abstraction over a concurrent work queue executor
pub trait ExecutorCore<J>: ExecutorHandle<J> + Sized {
    /// The handle into this executor exposed to running jobs
    type Handle<'a>: ExecutorHandle<J> + Copy + 'a;
}
