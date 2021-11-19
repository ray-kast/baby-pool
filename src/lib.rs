#![deny(
    clippy::suspicious,
    clippy::style,
    missing_debug_implementations,
    missing_copy_implementations,
    rustdoc::broken_intra_doc_links
)]
#![warn(clippy::pedantic, clippy::cargo, missing_docs)]

//! A simple threadpool for data-parallel concurrency.
//!
//! This crate is designed to parallelize breadth-first queue processing
//! operations, such as the following:
//!
//! ```
//! # use std::collections::VecDeque;
//! # #[derive(Debug, Clone, Copy)] struct FooData;
//! # #[derive(Debug, Clone, Copy)] struct BarData;
//! # const initial_data: FooData = FooData;
//! # fn process_foo(_: FooData) -> BarData { BarData }
//! #
//! enum Job {
//!     Foo(FooData),
//!     Bar(BarData),
//! }
//!
//! let mut q = VecDeque::new();
//!
//! q.push_back(Job::Foo(initial_data));
//!
//! while let Some(job) = q.pop_front() {
//!     match job {
//!         Job::Foo(foo) => {
//!             let bar = process_foo(foo);
//!             q.push_back(Job::Bar(bar));
//!         },
//!         Job::Bar(bar) => {
//!             println!("Bar: {:?}", bar);
//!         },
//!     }
//! }
//! ```
//!
//! Using `topograph`, the above can be written to use a threadpool like so:
//!
//! ```
//! # use std::collections::VecDeque;
//! # use topograph::{prelude::*, threaded};
//! # #[derive(Debug, Clone, Copy)] struct FooData;
//! # #[derive(Debug, Clone, Copy)] struct BarData;
//! # const initial_data: FooData = FooData;
//! # fn process_foo(_: FooData) -> BarData { BarData }
//! #
//! # enum Job {
//! #     Foo(FooData),
//! #     Bar(BarData),
//! # }
//! let pool = threaded::Builder::default().build(|job, handle| match job {
//!     Job::Foo(foo) => {
//!         let bar = process_foo(foo);
//!         handle.push(Job::Bar(bar));
//!     },
//!     Job::Bar(bar) => {
//!         // NOTE: this should be synchronized with a mutex
//!         println!("Bar: {:?}", bar);
//!     },
//! }).unwrap();
//!
//! pool.push(Job::Foo(initial_data));
//! pool.join();
//! ```

// pub mod graph;
pub mod threaded;

use std::panic::{RefUnwindSafe, UnwindSafe};

// pub use graph::ExecutorBuilderExt;

pub mod prelude {
    //! Common traits

    pub use super::{Executor, ExecutorBuilder, /* ExecutorBuilderExt, */ ExecutorHandle};
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
        work: impl Fn(J, &E::Handle) + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<E, Self::Error>;
}

/// Abstraction over a handle into an [`Executor`] accessible to concurrent jobs
pub trait ExecutorHandle<J>: UnwindSafe + RefUnwindSafe {
    /// Push a new job onto the executor for running as soon as possible
    fn push(&self, job: J);
}

/// Abstraction over a thread pool that
pub trait Executor<J: UnwindSafe>: Sized {
    /// The handle into this executor exposed to running jobs
    type Handle: ExecutorHandle<J>;

    /// Push a new job onto the executor for running as soon as possible
    fn push(&self, job: J);

    /// Disable pushing new jobs and wait for all pending work to complete,
    /// including jobs queued by currently-running jobs
    fn join(self);

    /// Disable pushing new jobs and wait for all currently-running jobs to
    /// finish before dropping the rest.
    fn abort(self);
}
