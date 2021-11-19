//! Threaded implementation of an executor

use std::{
    panic::{AssertUnwindSafe, RefUnwindSafe, UnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use dispose::abort_on_panic;
use parking_lot::{Condvar, Mutex};

use crate::prelude::*;

/// Builder for a threaded executor
#[derive(Clone, Copy, Debug, Default)]
pub struct Builder {
    num_threads: Option<usize>,
}

impl Builder {
    /// Specify the number of threads to use, or None to detect from `num_cpus`.
    pub fn num_threads(&mut self, num: impl Into<Option<usize>>) -> &mut Self {
        self.num_threads = num.into();
        self
    }
}

impl<J: Send + UnwindSafe + 'static> ExecutorBuilder<J, Executor<J>> for Builder {
    // TODO: when `!`
    type Error = std::convert::Infallible;

    fn build(
        self,
        f: impl Fn(J, &Handle<J>) + Send + Clone + RefUnwindSafe + 'static,
    ) -> Result<Executor<J>, Self::Error> {
        let Self { num_threads } = self;

        let num_threads = num_threads.unwrap_or_else(num_cpus::get);

        let work = (0..num_threads)
            .map(|i| (i, Worker::new_fifo()))
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
            live: Mutex::new(num_threads),
            unpark_var: Condvar::new(),
            join_var: Condvar::new(),
        });

        let threads = abort_on_panic({
            let core = &core;
            move || {
                work.into_iter()
                    .map(|(index, work)| {
                        std::thread::Builder::new()
                            .name(format!("Worker thread {}", index))
                            .spawn({
                                let core = core.clone();
                                let f = f.clone();

                                move || {
                                    WorkerThread {
                                        // index,
                                        work,
                                        core,
                                    }
                                    .run(f);
                                }
                            })
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            }
        });

        Ok(Executor(core, threads))
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
pub struct Handle<J>(AssertUnwindSafe<Arc<Core<J>>>);

/// Container and main executor for a FIFO thread pool.
///
/// Creating an instance of `ThreadPool` will spawn and park all the threads
/// necessary, and jobs will begin running as they are pushed.
#[derive(Debug)]
pub struct Executor<J>(Arc<Core<J>>, Vec<JoinHandle<()>>);

impl<J> Core<J> {
    /// **NOTE:** Use with care!  This is not atomic.
    fn is_empty(&self) -> bool { self.inj.is_empty() && self.steal.iter().all(Stealer::is_empty) }

    fn park(&self) {
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
    /// revoking outside access to the push() function.  This makes `is_empty` a
    /// sound approximation as no items can be added if no threads are live.
    fn join(&self) {
        let mut live = self.live.lock();

        while !(*live == 0 && self.is_empty()) {
            self.join_var.wait(&mut live);
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

impl<J> ExecutorHandle<J> for Handle<J> {
    fn push(&self, job: J) { self.0.push(job); }
}

impl<J> Executor<J> {
    fn join_threads(&mut self) {
        for handle in self.1.drain(..) {
            handle.join().unwrap();
        }
    }
}

impl<J: Send + UnwindSafe + 'static> crate::Executor<J> for Executor<J> {
    type Handle = Handle<J>;

    #[inline]
    fn push(&self, job: J) { self.0.push(job); }

    fn join(mut self) {
        self.0.join();
        self.join_threads();

        // Final sanity check
        assert!(self.0.is_empty(), "Thread pool starved!");
    }

    #[inline]
    fn abort(mut self) {
        self.0.abort();
        self.join_threads();
    }
}

impl<J> Drop for Executor<J> {
    fn drop(&mut self) { self.0.abort(); }
}

struct WorkerThread<J> {
    // Might expose in the future
    // index: usize,
    work: Worker<J>,
    core: Arc<Core<J>>,
}

impl<J: UnwindSafe> WorkerThread<J> {
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

    fn run(self, f: impl Fn(J, &Handle<J>) + RefUnwindSafe) {
        abort_on_panic(move || {
            // TODO: while it would be nice to borrow self.core, this isn't
            //       possible without GATs
            let handle = Handle(AssertUnwindSafe(self.core.clone()));

            while !self.core.stop.load(Ordering::Acquire) {
                if let Some(job) = self.get_job() {
                    match std::panic::catch_unwind(|| f(job, &handle)) {
                        Ok(()) => (),
                        Err(e) => log::error!("Job panicked: {:?}", e),
                    }
                } else {
                    self.core.park();
                }
            }
        });
    }
}
