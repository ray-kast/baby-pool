use std::{
    fmt::Debug,
    future::Future,
    ops,
    panic::{RefUnwindSafe, UnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use dispose::abort_on_panic;
use futures_util::FutureExt;

use crate::prelude::*;
use crate::nonblock::unwind::AbortOnPanic;

// TODO: check all trait bounds to see what can be relaxed

trait Runtime {
    // TODO: dumb issue with bounds and Debug requirements
    type Mutex<T: Debug + Send>: Debug + Send + Sync;
    type Condvar: Debug + Send + Sync;

    type JoinHandle<T>: Send;
    type JoinError: Debug;

    fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T>;

    fn new_condvar() -> Self::Condvar;

    fn notify_one(cvar: &Self::Condvar) -> bool;

    fn notify_all(cvar: &Self::Condvar) -> usize;
}

trait Spawn<F, J>: Runtime {
    type Output;
    type Error: std::error::Error;

    fn spawn(
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<Self::Output>, Self::Error>;
}

trait RuntimeSync: Runtime {
    type MutexGuard<'a, T: 'a>: ops::Deref<Target = T> + ops::DerefMut;

    fn lock<T: Debug + Send>(mutex: &Self::Mutex<T>) -> Self::MutexGuard<'_, T>;

    fn wait<T>(cvar: &Self::Condvar, guard: &mut Self::MutexGuard<'_, T>);

    fn join<T>(handle: Self::JoinHandle<T>) -> Result<T, Self::JoinError>;
}

trait RuntimeAsync: Runtime {
    type MutexGuard<'a, T>: ops::Deref<Target = T> + ops::DerefMut + Send;

    fn lock<T: Debug + Send>(
        mutex: &Self::Mutex<T>,
    ) -> impl Future<Output = Self::MutexGuard<'_, T>> + Send;

    fn wait<'a, T>(
        cvar: &'a Self::Condvar,
        guard: &mut Self::MutexGuard<'a, T>,
    ) -> impl Future<Output = ()> + Send;

    fn join<T>(
        handle: Self::JoinHandle<T>,
    ) -> impl Future<Output = Result<T, Self::JoinError>> + Send;
}

struct Threaded;

impl Runtime for Threaded {
    type Condvar = parking_lot::Condvar;
    type JoinError = Box<dyn std::any::Any + Send + 'static>;
    type JoinHandle<T> = std::thread::JoinHandle<T>;
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

impl<F: Fn(J, Handle<J, Self>) + RefUnwindSafe + Send + 'static, J: UnwindSafe + Send + 'static>
    Spawn<F, J> for Threaded
{
    type Error = std::io::Error;
    type Output = ();

    fn spawn(
        name: String,
        worker: WorkerThread<J, Self>,
        f: F,
    ) -> Result<Self::JoinHandle<Self::Output>, Self::Error> {
        std::thread::Builder::new()
            .name(name)
            .spawn(move || worker.run_sync(f))
    }
}

impl RuntimeSync for Threaded {
    type MutexGuard<'a, T: 'a> = parking_lot::MutexGuard<'a, T>;

    #[inline]
    fn lock<T: Debug + Send>(mutex: &Self::Mutex<T>) -> Self::MutexGuard<'_, T> { mutex.lock() }

    #[inline]
    fn wait<T>(cvar: &Self::Condvar, guard: &mut Self::MutexGuard<'_, T>) { cvar.wait(guard) }

    #[inline]
    fn join<T>(handle: Self::JoinHandle<T>) -> Result<T, Self::JoinError> { handle.join() }
}

struct Nonblock;

// impl Runtime for Nonblock {
//     type Mutex<T: Debug + Send>;

//     type Condvar;

//     type JoinHandle<T>;

//     type JoinError;

//     fn new_mutex<T: Debug + Send>(value: T) -> Self::Mutex<T> {
//         todo!()
//     }

//     fn new_condvar() -> Self::Condvar {
//         todo!()
//     }

//     fn notify_one(cvar: &Self::Condvar) -> bool {
//         todo!()
//     }

//     fn notify_all(cvar: &Self::Condvar) -> usize {
//         todo!()
//     }
// }

// impl<F, J> Spawn<F, J> for Nonblock {
//     type Output;

//     type Error;

//     fn spawn(
//         name: String,
//         worker: WorkerThread<J, Self>,
//         f: F,
//     ) -> Result<Self::JoinHandle<Self::Output>, Self::Error> {
//         todo!()
//     }
// }

// impl RuntimeAsync for Nonblock {
//     type MutexGuard<'a, T>;

//     fn lock<T: Debug + Send>(
//         mutex: &Self::Mutex<T>,
//     ) -> impl Future<Output = Self::MutexGuard<'_, T>> + Send {
//         todo!()
//     }

//     fn wait<T>(
//         cvar: &Self::Condvar,
//         guard: &mut Self::MutexGuard<'_, T>,
//     ) -> impl Future<Output = ()> + Send {
//         todo!()
//     }

//     fn join<T>(
//         handle: Self::JoinHandle<T>,
//     ) -> impl Future<Output = Result<T, Self::JoinError>> + Send {
//         todo!()
//     }
// }

/// Builder for a blocking executor
#[derive(Clone, Copy, Debug, Default)]
pub struct Builder {
    lifo: bool,
    num_threads: Option<usize>,
}

impl Builder {
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

impl Builder {
    // // TODO: when `!`
    // type Error = std::convert::Infallible;

    fn build<J, R: Spawn<F, J, Output = ()> + ?Sized, F: Clone>(self, f: F) -> Executor<J, R> {
        let Self { lifo, num_threads } = self;

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
            move || {
                work.into_iter()
                    .map(|(index, work)| {
                        let name = format!("Worker thread {index}");
                        let core = Arc::clone(core);

                        R::spawn(
                            name,
                            WorkerThread {
                                // index,
                                work,
                                core,
                            },
                            f.clone(),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
            }
        });

        Executor(core, handles)
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

impl<'a, J, R: Runtime + ?Sized> UnwindSafe for Handle<'a, J, R> {}
impl<'a, J, R: Runtime + ?Sized> RefUnwindSafe for Handle<'a, J, R> {}

/// Container and main executor for a FIFO thread pool.
///
/// Creating an instance of `ThreadPool` will spawn and park all the threads
/// necessary, and jobs will begin running as they are pushed.
#[derive(Debug)]
pub struct Executor<J, R: Runtime + ?Sized>(Arc<Core<J, R>>, Vec<R::JoinHandle<()>>);

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

impl<J, R: RuntimeSync + ?Sized> Core<J, R> {
    fn park_sync(&self) {
        if self.stop.load(Ordering::SeqCst) {
            return;
        }

        let mut live = R::lock(&self.live);
        *live -= 1;

        if *live == 0 {
            R::notify_all(&self.join_var);
        }

        R::wait(&self.unpark_var, &mut live);
        *live += 1;
    }

    /// # A note on soundness
    /// This only works because the exposed function consumes the thread pool,
    /// revoking outside access to the ``push()`` function.  This makes `is_empty` a
    /// sound approximation as no items can be added if no threads are live.
    fn join_sync(&self) {
        let mut live = R::lock(&self.live);

        while !(*live == 0 && self.is_empty()) {
            R::wait(&self.join_var, &mut live);
        }

        self.abort();
    }
}

impl<J, R: RuntimeAsync + ?Sized> Core<J, R> {
    async fn park_async(&self) {
        if self.stop.load(Ordering::SeqCst) {
            return;
        }

        let mut live = R::lock(&self.live).await;
        *live -= 1;

        if *live == 0 {
            R::notify_all(&self.join_var);
        }

        R::wait(&self.unpark_var, &mut live).await;
        *live += 1;
    }

    /// # A note on soundness
    /// This only works because the exposed function consumes the thread pool,
    /// revoking outside access to the ``push()`` function.  This makes `is_empty` a
    /// sound approximation as no items can be added if no threads are live.
    async fn join_async(&self) {
        let mut live = R::lock(&self.live).await;

        while !(*live == 0 && self.is_empty()) {
            R::wait(&self.join_var, &mut live).await;
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

impl<J, R: RuntimeSync + ?Sized> Executor<J, R> {
    fn join_sync(&mut self) {
        for handle in self.1.drain(..) {
            R::join(handle).unwrap();
        }
    }
}

impl<J, R: RuntimeAsync + ?Sized> Executor<J, R> {
    async fn join_async(&mut self) {
        for handle in self.1.drain(..) {
            R::join(handle).await.unwrap();
        }
    }
}

impl<J, R: Runtime + ?Sized> ExecutorHandle<J> for Executor<J, R> {
    #[inline]
    fn push(&self, job: J) { self.0.push(job); }
}

impl<J: Send + UnwindSafe + 'static, R: Runtime + ?Sized + 'static> ExecutorCore<J>
    for Executor<J, R>
{
    type Handle<'a> = Handle<'a, J, R>;
}

impl<J: Send + UnwindSafe + 'static, R: RuntimeSync + ?Sized + 'static> ExecutorSync<J>
    for Executor<J, R>
{
    fn join(mut self) {
        self.0.join_sync();
        self.join_sync();

        // Final sanity check
        assert!(self.0.is_empty(), "Thread pool starved!");
    }

    #[inline]
    fn abort(mut self) {
        self.0.abort();
        self.join_sync();
    }
}

impl<J: Send + UnwindSafe + 'static, R: RuntimeAsync + ?Sized + 'static> ExecutorAsync<J>
    for Executor<J, R>
{
    async fn join(mut self) {
        self.0.join_async().await;
        self.join_async().await;

        // Final sanity check
        assert!(self.0.is_empty(), "Thread pool starved!");
    }

    #[inline]
    async fn abort(mut self) {
        self.0.abort();
        self.join_async().await;
    }
}

impl<J, R: Runtime + ?Sized> Drop for Executor<J, R> {
    fn drop(&mut self) { self.0.abort(); }
}

struct WorkerThread<J, R: Runtime + ?Sized> {
    // Might expose in the future
    // index: usize,
    work: Worker<J>,
    core: Arc<Core<J, R>>,
}

impl<J: UnwindSafe, R: Runtime + ?Sized> WorkerThread<J, R> {
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

impl<J: UnwindSafe, R: RuntimeSync + ?Sized> WorkerThread<J, R> {
    fn run_sync(self, f: impl Fn(J, Handle<J, R>) + RefUnwindSafe) {
        abort_on_panic(move || {
            while !self.core.stop.load(Ordering::Acquire) {
                let handle = Handle(&self.core);
                if let Some(job) = self.get_job() {
                    match std::panic::catch_unwind(|| f(job, handle)) {
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

impl<J: UnwindSafe + Send, R: RuntimeAsync + ?Sized> WorkerThread<J, R> {
    fn run_async<F: Future<Output = ()> + UnwindSafe + Send>(
        self,
        f: impl Fn(J, Handle<J, R>) -> F + RefUnwindSafe + Send,
    ) -> impl Future<Output = ()> + Send {
        AbortOnPanic(async move {
            while !self.core.stop.load(Ordering::Acquire) {
                let handle = Handle(&self.core);
                if let Some(job) = self.get_job() {
                    match f(job, handle).catch_unwind().await {
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
