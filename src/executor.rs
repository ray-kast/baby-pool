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
};

// TODO: check all trait bounds to see what can be relaxed

pub trait Runtime {
    // TODO: dumb issue with bounds and Debug requirements
    type Mutex<T: Debug + Send>: Debug + Send + Sync;
    type Condvar: Debug + Send + Sync;

    type JoinHandle<T: Send>: Send;

    fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T>;

    fn new_condvar() -> Self::Condvar;

    fn notify_one(cvar: &Self::Condvar) -> bool;

    fn notify_all(cvar: &Self::Condvar) -> usize;
}

pub trait SpawnWorker<J, T>: Runtime + Sized {
    type Error: Error;

    fn spawn_worker<F: Fn(J, Handle<J, Self>) -> T + Send + 'static>(
        &self,
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::Error>;
}

pub trait AsyncExecutor: Send + Sync + 'static {
    type SpawnError: Error;

    type JoinHandle<T: Send>: Send;
    type JoinError: Error;

    fn spawn<F: Future<Output = ()> + Send + 'static>(
        &self,
        name: String,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::SpawnError>;

    fn join<T: Send>(
        handle: Self::JoinHandle<T>,
    ) -> impl Future<Output = Result<T, Self::JoinError>> + Send;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Blocking;

impl Runtime for Blocking {
    type Condvar = parking_lot::Condvar;
    type JoinHandle<T: Send> = std::thread::JoinHandle<T>;
    type Mutex<T: Debug + Send> = parking_lot::Mutex<T>;

    #[inline]
    fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T> { parking_lot::Mutex::new(value) }

    #[inline]
    fn new_condvar() -> Self::Condvar { parking_lot::Condvar::new() }

    #[inline]
    fn notify_one(cvar: &Self::Condvar) -> bool { cvar.notify_one() }

    #[inline]
    fn notify_all(cvar: &Self::Condvar) -> usize { cvar.notify_all() }
}

impl<J: Send + 'static> SpawnWorker<J, ()> for Blocking {
    type Error = std::io::Error;

    fn spawn_worker<F: Fn(J, Handle<J, Self>) + Send + 'static>(
        &self,
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::Error> {
        std::thread::Builder::new()
            .name(name)
            .spawn(move || worker.run_sync(f))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Nonblock<E>(pub E);

impl<E: AsyncExecutor> Runtime for Nonblock<E> {
    type Condvar = condvar::Condvar;
    type JoinHandle<T: Send> = E::JoinHandle<T>;
    type Mutex<T: Debug + Send> = condvar::Mutex<T>;

    #[inline]
    fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T> { condvar::Mutex::new(value) }

    fn new_condvar() -> Self::Condvar { condvar::Condvar::new() }

    fn notify_one(cvar: &Self::Condvar) -> bool { cvar.notify_one() }

    fn notify_all(cvar: &Self::Condvar) -> usize { cvar.notify_all() }
}

impl<J: Send + 'static, T: Send + Future<Output = ()> + 'static, E: AsyncExecutor> SpawnWorker<J, T>
    for Nonblock<E>
{
    type Error = E::SpawnError;

    #[inline]
    fn spawn_worker<F: Fn(J, Handle<J, Self>) -> T + Send + 'static>(
        &self,
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<()>, Self::Error> {
        self.0.spawn(name, worker.run_async(f))
    }
}

#[cfg(feature = "tokio")]
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
    num_threads: Option<usize>,
    runtime: R,
    phantom: PhantomData<fn(J)>,
}

impl<J, R: Default> Default for Builder<J, R> {
    #[inline]
    fn default() -> Self { Self::new(R::default()) }
}

impl<J, R> Builder<J, R> {
    pub fn new(runtime: R) -> Self {
        Self {
            lifo: false,
            num_threads: None,
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
    pub fn num_threads(mut self, num: impl Into<Option<usize>>) -> Self {
        self.num_threads = num.into();
        self
    }
}

// TODO: remove usages of impl Trait where it muddies the API

impl<J: Send + 'static, T, R: SpawnWorker<J, T> + 'static> ExecutorBuilder<J, T> for Builder<J, R> {
    type Error = R::Error;
    type Executor = Executor<J, R>;

    fn build<
        F: Fn(J, <Self::Executor as ExecutorCore<J>>::Handle<'_>) -> T + Clone + Send + 'static,
    >(
        self,
        f: F,
    ) -> Result<Executor<J, R>, R::Error> {
        let Self {
            lifo,
            num_threads,
            runtime,
            phantom: _,
        } = self;

        let num_threads = num_threads.unwrap_or_else(num_cpus::get);

        let work = (0..num_threads)
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
            live: R::new_mutex(num_threads),
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

impl<'a, J, R: Runtime + ?Sized> Copy for Handle<'a, J, R> {}
impl<'a, J, R: Runtime + ?Sized> Clone for Handle<'a, J, R> {
    fn clone(&self) -> Self { *self }
}

/// Container and main executor for a FIFO thread pool.
///
/// Creating an instance of `ThreadPool` will spawn and park all the threads
/// necessary, and jobs will begin running as they are pushed.
#[derive(Debug)]
pub struct Executor<J, R: Runtime + ?Sized>(Arc<Core<J, R>>, Vec<R::JoinHandle<()>>);

impl<J, R: Runtime> Executor<J, R> {
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

impl<'a, J, R: Runtime + ?Sized> ExecutorHandle<J> for Handle<'a, J, R> {
    #[inline]
    fn push(&self, job: J) { self.0.push(job); }
}

impl<J, R: Runtime + ?Sized> Executor<J, R> {
    /// Get the number of threads created for this executor
    #[must_use]
    pub fn num_threads(&self) -> usize { self.1.len() }
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
    pub fn join(mut self) {
        self.0.join_sync();
        self.join_handles_sync();

        // Final sanity check
        assert!(self.0.is_empty(), "Thread pool starved!");
    }

    #[inline]
    pub fn abort(mut self) {
        self.0.abort();
        self.join_handles_sync();
    }
}

impl<J: Send + 'static, E: AsyncExecutor> Executor<J, Nonblock<E>> {
    pub async fn join_async(mut self) {
        self.0.join_async().await;
        self.join_handles_async().await;

        // Final sanity check
        assert!(self.0.is_empty(), "Thread pool starved!");
    }

    #[inline]
    pub async fn abort_async(mut self) {
        self.0.abort();
        self.join_handles_async().await;
    }
}

impl<J, R: Runtime + ?Sized> Drop for Executor<J, R> {
    fn drop(&mut self) { self.0.abort(); }
}

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

impl<J: Send, E: AsyncExecutor> WorkerThread<J, Nonblock<E>> {
    fn run_async<F: Future<Output = ()> + Send>(
        self,
        f: impl Fn(J, Handle<J, Nonblock<E>>) -> F + Send,
    ) -> impl Future<Output = ()> + Send {
        AbortOnPanic(async move {
            while !self.core.stop.load(Ordering::Acquire) {
                let handle = Handle(&self.core);
                if let Some(job) = self.get_job() {
                    match AssertUnwindSafe(f(job, handle)).catch_unwind().await {
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
