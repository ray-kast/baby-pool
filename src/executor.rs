//! Functionality for constructing and running work-queue executors
//!
//! This module exposes the [`Executor`] type, which can be built from
//! [`Blocking`] or [`Nonblock`] runtime tags using [`Executor::builder`].
//!
//! # Example
//!
//! ```
//! # use topograph::{prelude::*, executor};
//! struct Job(&'static str);
//!
//! let pool = executor::Builder::new(executor::Blocking).build(|job, handle| {
//!     let Job(job) = job;
//!     println!("{}", job);
//! }).unwrap();
//!
//! pool.push(Job("foo"));
//! pool.push(Job("bar"));
//! pool.push(Job("baz"));
//! ```
//!
//! The above code will print `foo`, `bar`, and `baz`, but not in any
//! guaranteed order as the jobs may be dequeued in parallel.

use std::{
    error::Error,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use dispose::abort_on_panic;
use futures_util::FutureExt;

use crate::{
    nonblock::{condvar, unwind::AbortOnPanic},
    prelude::*,
    AsyncHandler, ExecutorBuilder,
};

// TODO: check all trait bounds to see what can be relaxed

/// A minimal description of a concurrent runtime that can host `topograph`.
/// This trait is designed to be consumed internally by `topograph`'s thread
/// scheduler.
pub trait Runtime {
    /// The mutex type compatible with this runtime
    // TODO: dumb issue with bounds and Debug requirements
    type Mutex<T: Debug + Send>: Debug + Send + Sync;
    /// The condition variable type compatible with this runtime
    type Condvar: Debug + Send + Sync;

    /// The type of handle returned by tasks spawned into this runtime
    type JoinHandle<T: Send>: Send;

    /// The type of error that may be returned when spawning tasks into this
    /// runtime
    type SpawnError: Error;

    /// Construct a new mutex for this runtime with the given value
    fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T>;

    /// Construct a new condition variable for this runtime
    fn new_condvar() -> Self::Condvar;

    /// Notify a single waiter on the given condition variable to wake
    fn notify_one(cvar: &Self::Condvar) -> bool;

    /// Notify all waiters on the given condition variable to wake
    fn notify_all(cvar: &Self::Condvar) -> usize;
}

/// A minimal description of a concurrent runtime that can spawn worker tasks
/// for `topograph`
pub trait SpawnWorker<J, F>: Runtime + Sized {
    /// Spawn a new task to run the given worker with the given user-provided
    /// task
    ///
    /// # Errors
    /// This function may return an error if a task was unable to be spawned in
    /// the runtime
    fn spawn_worker(
        &self,
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::SpawnError>;
}

/// A minimal description of the interface required to spawn and join tasks
/// running on an async executor
pub trait AsyncExecutor: Send + Sync + 'static {
    /// The type of error that may be returned when spawning a task
    type SpawnError: Error;

    /// The type of handle returned by a spawned task
    type JoinHandle<T: Send>: Send;
    /// The type of error that may be returned when joining a task
    type JoinError: Error;

    /// Spawn a new asynchronous task returning unit
    ///
    /// # Errors
    /// This function may return an error if the async runtime could not spawn
    /// the task
    fn spawn<F: Future<Output = ()> + Send + 'static>(
        &self,
        name: String,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::SpawnError>;

    /// Join a running task, blocking until it completes
    ///
    /// # Errors
    /// This function may return an error if the task to be joined panicked
    fn join<T: Send>(
        handle: Self::JoinHandle<T>,
    ) -> impl Future<Output = Result<T, Self::JoinError>> + Send;
}

/// Runtime traits for spawning a blocking [`Executor`]
#[derive(Debug, Default, Clone, Copy)]
pub struct Blocking;

impl Runtime for Blocking {
    type Condvar = parking_lot::Condvar;
    type JoinHandle<T: Send> = std::thread::JoinHandle<T>;
    type Mutex<T: Debug + Send> = parking_lot::Mutex<T>;
    type SpawnError = std::io::Error;

    #[inline]
    fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T> { parking_lot::Mutex::new(value) }

    #[inline]
    fn new_condvar() -> Self::Condvar { parking_lot::Condvar::new() }

    #[inline]
    fn notify_one(cvar: &Self::Condvar) -> bool { cvar.notify_one() }

    #[inline]
    fn notify_all(cvar: &Self::Condvar) -> usize { cvar.notify_all() }
}

impl<J: Send + 'static, F: Fn(J, Handle<J, Self>) + Send + 'static> SpawnWorker<J, F> for Blocking {
    fn spawn_worker(
        &self,
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::SpawnError> {
        std::thread::Builder::new()
            .name(name)
            .spawn(move || worker.run_sync(f))
    }
}

/// Runtime traits for spawning a non-blocking [`Executor`] given an
/// [async executor](AsyncExecutor)
#[derive(Debug, Default, Clone, Copy)]
pub struct Nonblock<E>(pub E);

impl<E: AsyncExecutor> Runtime for Nonblock<E> {
    type Condvar = condvar::Condvar;
    type JoinHandle<T: Send> = E::JoinHandle<T>;
    type Mutex<T: Debug + Send> = condvar::Mutex<T>;
    type SpawnError = E::SpawnError;

    #[inline]
    fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T> { condvar::Mutex::new(value) }

    fn new_condvar() -> Self::Condvar { condvar::Condvar::new() }

    fn notify_one(cvar: &Self::Condvar) -> bool { cvar.notify_one() }

    fn notify_all(cvar: &Self::Condvar) -> usize { cvar.notify_all() }
}

impl<
        J: Send + 'static,
        E: AsyncExecutor,
        F: for<'h> AsyncHandler<J, Handle<'h, J, Nonblock<E>>, Output = ()> + Send + 'static,
    > SpawnWorker<J, F> for Nonblock<E>
{
    #[inline]
    fn spawn_worker(
        &self,
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::SpawnError> {
        self.0.spawn(name, worker.run_async(f))
    }
}

#[cfg(feature = "tokio")]
/// Runtime traits for spawning tasks on a Tokio runtime
#[derive(Debug, Default, Clone, Copy)]
pub struct Tokio;

#[cfg(feature = "tokio")]
impl AsyncExecutor for Tokio {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: Send> = tokio::task::JoinHandle<T>;
    type SpawnError = std::convert::Infallible;

    #[inline]
    fn spawn<F: Future<Output = ()> + Send + 'static>(
        &self,
        _name: String,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::SpawnError> {
        Ok(tokio::task::spawn(f))
    }

    #[inline]
    fn join<T: Send>(
        handle: Self::JoinHandle<T>,
    ) -> impl Future<Output = Result<T, Self::JoinError>> + Send {
        handle
    }
}

/// Builder for a blocking executor
#[derive(Clone, Copy, Debug)]
pub struct Builder<J, R> {
    lifo: bool,
    max_concurrency: Option<usize>,
    runtime: R,
    phantom: PhantomData<fn(J)>,
}

impl<J, R: Default> Default for Builder<J, R> {
    #[inline]
    fn default() -> Self { Self::new(R::default()) }
}

impl<J, R> Builder<J, R> {
    /// Construct a new executor builder with the given runtime and default
    /// FIFO configuration
    pub fn new(runtime: R) -> Self {
        Self {
            lifo: false,
            max_concurrency: None,
            runtime,
            phantom: PhantomData,
        }
    }

    /// Specify whether this executor should use a LIFO queue (default is FIFO).
    #[must_use]
    pub fn lifo(mut self, lifo: bool) -> Self {
        self.lifo = lifo;
        self
    }

    /// Specify the number of threads to use, or None to detect from `num_cpus`.
    #[must_use]
    pub fn max_concurrency(mut self, num: impl Into<Option<usize>>) -> Self {
        self.max_concurrency = num.into();
        self
    }
}

// TODO: remove usages of impl Trait where it muddies the API

impl<J: Send + 'static, R: Runtime + 'static> ExecutorBuilderCore<J> for Builder<J, R> {
    type Error = R::SpawnError;
    type Executor = Executor<J, R>;
}

impl<J: Send + 'static, R: SpawnWorker<J, F> + 'static, F: Clone> ExecutorBuilder<J, F>
    for Builder<J, R>
{
    fn build(self, f: F) -> Result<Executor<J, R>, R::SpawnError> {
        let Self {
            lifo,
            max_concurrency,
            runtime,
            phantom: _,
        } = self;

        let max_concurrency = max_concurrency.unwrap_or_else(num_cpus::get);

        let work = (0..max_concurrency)
            .map(|i| {
                (
                    i,
                    if lifo {
                        Worker::new_lifo()
                    } else {
                        Worker::new_fifo()
                    },
                )
            })
            .collect::<Vec<_>>();

        let steal = work
            .iter()
            .map(|(_, w)| w.stealer())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let core = Arc::new(Core {
            inj: Injector::new(),
            steal,
            stop: AtomicBool::new(false),
            live: R::new_mutex(max_concurrency),
            unpark_var: R::new_condvar(),
            join_var: R::new_condvar(),
        });

        let handles = abort_on_panic({
            let core = &core;
            let runtime = &runtime;
            move || {
                work.into_iter()
                    .map(|(index, work)| {
                        let name = format!("Worker thread {index}");
                        let core = Arc::clone(core);

                        runtime.spawn_worker(
                            name,
                            WorkerThread {
                                // index,
                                work,
                                core,
                            },
                            f.clone(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        })?;

        Ok(Executor(core, handles))
    }
}

#[derive(Debug)]
struct Core<J, R: Runtime + ?Sized> {
    inj: Injector<J>,
    steal: Box<[Stealer<J>]>,
    live: R::Mutex<usize>,
    unpark_var: R::Condvar,
    join_var: R::Condvar,
    stop: AtomicBool,
}

/// Thread-safe view into a thread pool, for access by running jobs.
///
/// This view allows you to push additional jobs, but does not expose any
/// functionality that could interfere with a running thread pool.
#[derive(Debug)]
pub struct Handle<'a, J, R: Runtime + ?Sized>(&'a Arc<Core<J, R>>);

impl<J, R: Runtime + ?Sized> Copy for Handle<'_, J, R> {}
impl<J, R: Runtime + ?Sized> Clone for Handle<'_, J, R> {
    fn clone(&self) -> Self { *self }
}

/// Container and main executor for a FIFO thread pool.
///
/// Creating an instance of `ThreadPool` will spawn and park all the threads
/// necessary, and jobs will begin running as they are pushed.
#[derive(Debug)]
pub struct Executor<J, R: Runtime + ?Sized>(Arc<Core<J, R>>, Vec<R::JoinHandle<()>>);

impl<J, R: Runtime> Executor<J, R> {
    /// Construct a new executor builder with the given runtime and default
    /// FIFO configuration
    #[inline]
    pub fn builder(runtime: R) -> Builder<J, R> { Builder::new(runtime) }
}

impl<J, R: Runtime + ?Sized> Core<J, R> {
    /// **NOTE:** Use with care!  This is not atomic.
    fn is_empty(&self) -> bool { self.inj.is_empty() && self.steal.iter().all(Stealer::is_empty) }

    fn abort(&self) {
        self.stop.store(true, Ordering::SeqCst);
        R::notify_all(&self.unpark_var);
    }

    fn push(&self, job: J) {
        self.inj.push(job);
        R::notify_one(&self.unpark_var);
    }
}

impl<J> Core<J, Blocking> {
    fn park_sync(&self) {
        if self.stop.load(Ordering::SeqCst) {
            return;
        }

        let mut live = self.live.lock();
        *live -= 1;

        if *live == 0 {
            self.join_var.notify_all();
        }

        self.unpark_var.wait(&mut live);
        *live += 1;
    }

    /// # A note on soundness
    /// This only works because the exposed function consumes the thread pool,
    /// revoking outside access to the ``push()`` function.  This makes `is_empty` a
    /// sound approximation as no items can be added if no threads are live.
    fn join_sync(&self) {
        let mut live = self.live.lock();

        while !(*live == 0 && self.is_empty()) {
            self.join_var.wait(&mut live);
        }

        self.abort();
    }
}

impl<J, E: AsyncExecutor> Core<J, Nonblock<E>> {
    async fn park_async(&self) {
        if self.stop.load(Ordering::SeqCst) {
            return;
        }

        let mut live = self.live.lock().await;
        *live -= 1;

        if *live == 0 {
            self.join_var.notify_all();
        }

        self.unpark_var.wait(&mut live).await;
        *live += 1;
    }

    /// # A note on soundness
    /// This only works because the exposed function consumes the thread pool,
    /// revoking outside access to the ``push()`` function.  This makes `is_empty` a
    /// sound approximation as no items can be added if no threads are live.
    async fn join_async(&self) {
        let mut live = self.live.lock().await;

        while !(*live == 0 && self.is_empty()) {
            self.join_var.wait(&mut live).await;
        }

        self.abort();
    }
}

impl<J, R: Runtime + ?Sized> ExecutorHandle<J> for Handle<'_, J, R> {
    #[inline]
    fn push(&self, job: J) { self.0.push(job); }
}

impl<J, R: Runtime + ?Sized> Executor<J, R> {
    /// Get the number of tasks created for this executor
    #[must_use]
    pub fn max_concurrency(&self) -> usize { self.1.len() }
}

impl<J> Executor<J, Blocking> {
    fn join_handles_sync(&mut self) {
        for handle in self.1.drain(..) {
            handle.join().unwrap();
        }
    }
}

impl<J, E: AsyncExecutor> Executor<J, Nonblock<E>> {
    async fn join_handles_async(&mut self) {
        for handle in self.1.drain(..) {
            E::join(handle).await.unwrap();
        }
    }
}

impl<J, R: Runtime + ?Sized> ExecutorHandle<J> for Executor<J, R> {
    #[inline]
    fn push(&self, job: J) { self.0.push(job); }
}

impl<J: Send + 'static, R: Runtime + ?Sized + 'static> ExecutorCore<J> for Executor<J, R> {
    type Handle<'a> = Handle<'a, J, R>;
}

impl<J: Send + 'static> Executor<J, Blocking> {
    /// Disable pushing new jobs and wait for all pending work to complete,
    /// including jobs queued by currently-running jobs
    pub fn join(mut self) {
        self.0.join_sync();
        self.join_handles_sync();

        // Final sanity check
        if !self.0.is_empty() {
            unreachable!("Thread pool starved!");
        }
    }

    /// Disable pushing new jobs and wait for all currently-running jobs to
    /// finish before dropping the rest
    #[inline]
    pub fn abort(mut self) {
        self.0.abort();
        self.join_handles_sync();
    }
}

impl<J: Send + 'static, E: AsyncExecutor> Executor<J, Nonblock<E>> {
    /// Returns a future that disables pushing new jobs and yields after all
    /// pending work has completed, including jobs queued by currently-running
    /// jobs
    pub async fn join_async(mut self) {
        self.0.join_async().await;
        self.join_handles_async().await;

        // Final sanity check
        if !self.0.is_empty() {
            unreachable!("Thread pool starved!");
        }
    }

    /// Returns a future that disables pushing new jobs and yields after all
    /// currently-running jobs have finished, dropping the rest
    #[inline]
    pub async fn abort_async(mut self) {
        self.0.abort();
        self.join_handles_async().await;
    }
}

impl<J, R: Runtime + ?Sized> Drop for Executor<J, R> {
    fn drop(&mut self) { self.0.abort(); }
}

/// A description of a single `topograph` worker task.  This struct is intended
/// to be used internally or through the [`SpawnWorker`] trait
#[derive(Debug)]
pub struct WorkerThread<J, R: Runtime + ?Sized> {
    // Might expose in the future
    // index: usize,
    work: Worker<J>,
    core: Arc<Core<J, R>>,
}

impl<J, R: Runtime + ?Sized> WorkerThread<J, R> {
    fn get_job(&self) -> Option<J> {
        self.work.pop().or_else(|| {
            let WorkerThread { work, .. } = self;
            let Core {
                ref stop,
                ref inj,
                ref steal,
                ..
            } = *self.core;

            loop {
                if stop.load(Ordering::Acquire) {
                    break None;
                }

                match inj
                    .steal_batch_and_pop(work)
                    .or_else(|| steal.iter().map(Stealer::steal).collect())
                {
                    Steal::Empty => break None,
                    Steal::Success(job) => break Some(job),
                    Steal::Retry => (),
                }
            }
        })
    }
}

impl<J> WorkerThread<J, Blocking> {
    fn run_sync(self, f: impl Fn(J, Handle<J, Blocking>)) {
        abort_on_panic(move || {
            while !self.core.stop.load(Ordering::Acquire) {
                let handle = Handle(&self.core);
                if let Some(job) = self.get_job() {
                    match std::panic::catch_unwind(AssertUnwindSafe(|| f(job, handle))) {
                        Ok(()) => (),
                        Err(e) => log::error!("Job panicked: {:?}", e),
                    }
                } else {
                    self.core.park_sync();
                }
            }
        });
    }
}

impl<J: Send + 'static> ExecutorBuilderSync<J> for Builder<J, Blocking> {
    #[inline]
    fn build<F: Fn(J, <Self::Executor as ExecutorCore<J>>::Handle<'_>) + Clone + Send + 'static>(
        self,
        work: F,
    ) -> Result<Self::Executor, Self::Error> {
        ExecutorBuilder::build(self, work)
    }
}

impl<J: Send + 'static, E: AsyncExecutor> ExecutorBuilderAsync<J> for Builder<J, Nonblock<E>> {
    #[inline]
    fn build_async<
        F: for<'h> AsyncHandler<J, <Self::Executor as ExecutorCore<J>>::Handle<'h>, Output = ()>
            + Clone
            + Send
            + 'static,
    >(
        self,
        work: F,
    ) -> Result<Self::Executor, Self::Error> {
        ExecutorBuilder::build(self, work)
    }
}

impl<J: Send, E: AsyncExecutor> WorkerThread<J, Nonblock<E>> {
    fn run_async<F: for<'h> AsyncHandler<J, Handle<'h, J, Nonblock<E>>, Output = ()> + Send>(
        self,
        f: F,
    ) -> impl Future<Output = ()> + Send {
        AbortOnPanic(async move {
            while !self.core.stop.load(Ordering::Acquire) {
                let handle = Handle(&self.core);
                if let Some(job) = self.get_job() {
                    match AssertUnwindSafe(f.handle(job, handle)).catch_unwind().await {
                        Ok(()) => (),
                        Err(e) => log::error!("Job panicked: {:?}", e),
                    }
                } else {
                    self.core.park_async().await;
                }
            }
        })
    }
}
