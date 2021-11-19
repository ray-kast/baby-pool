//! Concurrent implementation of toposort

use std::{
    marker::PhantomData,
    panic::{RefUnwindSafe, UnwindSafe},
};

use crate::prelude::*;

/// A job payload and associated dependency information
#[derive(Debug)]
pub struct Job<J>(J);

/// A handle into the graph scheduler for running jobs
#[derive(Debug)]
pub struct Handle<H>(H);

/// Job scheduler using topological sort to manage dependencies
#[derive(Debug)]
pub struct Scheduler<J, E> {
    executor: E,
    _m: PhantomData<J>,
}

impl<J, H: ExecutorHandle<Job<J>>> ExecutorHandle<J> for Handle<H> {
    fn push(&self, job: J) { self.0.push(Job(job)); }
}

impl<'a, J: UnwindSafe, E: Executor<Job<J>>> Scheduler<J, E> {
    /// Construct a new graph scheduler
    fn new<B: ExecutorBuilder<Job<J>, E>>(
        b: B,
        f: impl Fn(J, &E::Handle) -> Result<(), ()> + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<Self, B::Error> {
        let executor = b.build(move |Job(job), handle| match f(job, handle) {
            Ok(()) => {
                // TODO: signal dependents
            },
            Err(()) => (),
        })?;

        Ok(Self {
            executor,
            _m: PhantomData::default(),
        })
    }
}

/// Adds the [`build_graph`] method to [`ExecutorBuilder`]
pub trait ExecutorBuilderExt<J: UnwindSafe, E: Executor<Job<J>>>:
    Sized + ExecutorBuilder<Job<J>, E>
{
    /// Construct a new graph scheduler using this builder's executor type
    fn build_graph(
        self,
        work: impl Fn(J, &E::Handle) -> Result<(), ()> + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<Scheduler<J, E>, Self::Error>;
}

impl<J: UnwindSafe, E: Executor<Job<J>>, B: ExecutorBuilder<Job<J>, E> + Sized>
    ExecutorBuilderExt<J, E> for B
{
    fn build_graph(
        self,
        work: impl Fn(J, &E::Handle) -> Result<(), ()> + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<Scheduler<J, E>, Self::Error> {
        Scheduler::new(self, work)
    }
}

impl<J: UnwindSafe, E: Executor<Job<J>>> Executor<J> for Scheduler<J, E> {
    type Handle = Handle<E::Handle>;

    #[inline]
    fn push(&self, job: J) { self.executor.push(Job(job)); }

    #[inline]
    fn join(self) { self.executor.join(); }

    #[inline]
    fn abort(self) { self.executor.abort(); }
}
