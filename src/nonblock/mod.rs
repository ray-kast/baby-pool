use std::{
    future::Future,
    panic::{RefUnwindSafe, UnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use dispose::abort_on_panic;

use self::{
    condvar::{Condvar, Mutex},
    runtime::{JoinHandle, Runtime},
};
use crate::prelude::*;

mod condvar;
mod runtime;
pub mod unwind;

// TODO: remove all instances of "thread" and "blocking"

/// Builder for a non-blocking executor
#[derive(Clone, Copy, Debug)]
pub struct Builder<R> {
    lifo: bool,
    num_tasks: Option<usize>,
    runtime: R,
}

impl<R: Default> Default for Builder<R> {
    #[inline]
    fn default() -> Self { Self::from_runtime(R::default()) }
}

impl<R> Builder<R> {
    pub const fn from_runtime(runtime: R) -> Self {
        Self {
            lifo: false,
            num_tasks: None,
            runtime,
        }
    }

    /// Specify whether this executor should use a LIFO queue (default is FIFO).
    #[must_use]
    pub fn lifo(mut self, lifo: bool) -> Self {
        self.lifo = lifo;
        self
    }

    /// Specify the number of tasks to run concurrently, or None to detect from `num_cpus`.
    #[must_use]
    pub fn num_tasks(mut self, num: impl Into<Option<usize>>) -> Self {
        self.num_tasks = num.into();
        self
    }
}

impl<J: Send + UnwindSafe + 'static, R: Runtime, F: Future<Output = ()>>
    ExecutorBuilder<J, Executor<J>, F> for Builder<R>
{
    // TODO: when `!`
    type Error = std::convert::Infallible;

    fn build(
        self,
        f: impl Fn(J, Handle<J>) -> F + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<Executor<J>, Self::Error> {
        let Self {
            lifo,
            num_tasks,
            runtime,
        } = self;

        let num_tasks = num_tasks.unwrap_or_else(num_cpus::get);

        let work = (0..num_tasks)
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
            live: Mutex::new(num_tasks),
            unpark_var: Condvar::new(),
            join_var: Condvar::new(),
        });

        let tasks = abort_on_panic({
            let core = &core;
            move || {
                work.into_iter()
                    .map(|(index, work)| {
                        runtime
                            .spawn(format!("Worker task {index}"), {
                                let core = Arc::clone(core);
                                let f = f.clone();

                                WorkerTask {
                                    // index,
                                    work,
                                    core,
                                }
                                .run(f)
                            })
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            }
        });

        Ok(Executor(core, tasks))
    }
}

#[derive(Debug)]
struct Core<J> {
    inj: Injector<J>,
    steal: Box<[Stealer<J>]>,
    live: Mutex<usize>,
    unpark_var: Condvar,
    join_var: Condvar,
    stop: AtomicBool,
}

/// Thread-safe view into a thread pool, for access by running jobs.
///
/// This view allows you to push additional jobs, but does not expose any
/// functionality that could interfere with a running thread pool.
#[derive(Debug)]
pub struct Handle<'a, J>(&'a Arc<Core<J>>);

impl<'a, J> Copy for Handle<'a, J> {}
impl<'a, J> Clone for Handle<'a, J> {
    fn clone(&self) -> Self { *self }
}

impl<'a, J> UnwindSafe for Handle<'a, J> {}
impl<'a, J> RefUnwindSafe for Handle<'a, J> {}

/// Container and main executor for a FIFO thread pool.
///
/// Creating an instance of `ThreadPool` will spawn and park all the threads
/// necessary, and jobs will begin running as they are pushed.
#[derive(Debug)]
pub struct Executor<J>(Arc<Core<J>>, Vec<JoinHandle<()>>);

impl<J> Core<J> {
    /// **NOTE:** Use with care!  This is not atomic.
    fn is_empty(&self) -> bool { self.inj.is_empty() && self.steal.iter().all(Stealer::is_empty) }

    async fn park(&self) {
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
    async fn join(&self) {
        let mut live = self.live.lock().await;

        while !(*live == 0 && self.is_empty()) {
            self.join_var.wait(&mut live).await;
        }

        self.abort();
    }

    fn abort(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.unpark_var.notify_all();
    }

    fn push(&self, job: J) {
        self.inj.push(job);
        self.unpark_var.notify_one();
    }
}

impl<'a, J> ExecutorHandle<J> for Handle<'a, J> {
    #[inline]
    fn push(&self, job: J) { self.0.push(job); }
}

impl<J> Executor<J> {
    /// Get the number of threads created for this executor
    #[must_use]
    pub fn num_tasks(&self) -> usize { self.1.len() }

    async fn join_tasks(&mut self) {
        for handle in self.1.drain(..) {
            handle.await.unwrap();
        }
    }
}

impl<J> ExecutorHandle<J> for Executor<J> {
    #[inline]
    fn push(&self, job: J) { self.0.push(job); }
}

impl<J: Send + UnwindSafe + 'static> ExecutorCore<J> for Executor<J> {
    type Handle<'a> = Handle<'a, J>;
}

impl<J: Send + UnwindSafe + 'static> ExecutorAsync<J> for Executor<J> {
    async fn join(mut self) {
        self.0.join();
        self.join_tasks();

        // Final sanity check
        assert!(self.0.is_empty(), "Thread pool starved!");
    }

    #[inline]
    async fn abort(mut self) {
        self.0.abort();
        self.join_tasks();
    }
}

impl<J> Drop for Executor<J> {
    fn drop(&mut self) { self.0.abort(); }
}

struct WorkerTask<J> {
    // Might expose in the future
    // index: usize,
    work: Worker<J>,
    core: Arc<Core<J>>,
}

impl<J: UnwindSafe> WorkerTask<J> {
    fn get_job(&self) -> Option<J> {
        self.work.pop().or_else(|| {
            let WorkerTask { work, .. } = self;
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

    async fn run<F: Future<Output = ()>>(self, f: impl Fn(J, Handle<J>) -> F + RefUnwindSafe) {
        // TODO: propagate abort_on_panic
        abort_on_panic(move || async move {
            while !self.core.stop.load(Ordering::Acquire) {
                let handle = Handle(&self.core);
                if let Some(job) = self.get_job() {
                    match std::panic::catch_unwind(|| f(job, handle)) {
                        Ok(f) => {
                            f.await;
                            todo!("catch_unwind");
                        },
                        Err(e) => log::error!("Job panicked: {:?}", e),
                    }
                } else {
                    self.core.park();
                }
            }
        });
    }
}
