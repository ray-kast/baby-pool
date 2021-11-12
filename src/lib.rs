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
//! Using `baby_pool`, the above can be written to use a threadpool like so:
//!
//! ```
//! # use std::collections::VecDeque;
//! # use baby_pool::ThreadPool;
//! # #[derive(Debug, Clone, Copy)] struct FooData;
//! # #[derive(Debug, Clone, Copy)] struct BarData;
//! # const initial_data: FooData = FooData;
//! # fn process_foo(_: FooData) -> BarData { BarData }
//! #
//! # enum Job {
//! #     Foo(FooData),
//! #     Bar(BarData),
//! # }
//! let pool = ThreadPool::new(None, |job, handle| match job {
//!     Job::Foo(foo) => {
//!         let bar = process_foo(foo);
//!         handle.push(Job::Bar(bar));
//!     },
//!     Job::Bar(bar) => {
//!         // NOTE: this should be synchronized with a mutex
//!         println!("Bar: {:?}", bar);
//!     },
//! });
//!
//! pool.push(Job::Foo(initial_data));
//! pool.join();
//! ```

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

#[derive(Debug)]
struct ThreadPoolCore<J> {
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
pub struct ThreadPoolHandle<'a, J>(AssertUnwindSafe<&'a Arc<ThreadPoolCore<J>>>);

/// Container and main executor for a FIFO thread pool.
///
/// Creating an instance of `ThreadPool` will spawn and park all the threads
/// necessary, and jobs will begin running as they are pushed.
#[derive(Debug)]
pub struct ThreadPool<J>(Arc<ThreadPoolCore<J>>, Vec<JoinHandle<()>>);

impl<J> ThreadPoolCore<J> {
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

impl<'a, J> ThreadPoolHandle<'a, J> {
    /// Push `job` onto the end of the work queue for this thread pool.
    pub fn push(&self, job: J) { self.0.push(job); }
}

impl<J: Send + UnwindSafe + 'static> ThreadPool<J> {
    /// Construct a new thread pool and spawn the worker threads.
    ///
    /// If no value is provided for `num_threads`, the number of detected CPU
    /// cores on the system will be used.
    ///
    /// # Panics
    /// This function panics and aborts the process if a thread cannot be
    /// created successfully.
    pub fn new(
        num_threads: impl Into<Option<usize>>,
        f: impl Fn(J, ThreadPoolHandle<J>) + Send + Clone + RefUnwindSafe + 'static,
    ) -> Self {
        let num_threads = num_threads.into().unwrap_or_else(num_cpus::get);

        let work = (0..num_threads)
            .map(|i| (i, Worker::new_fifo()))
            .collect::<Vec<_>>();

        let steal = work
            .iter()
            .map(|(_, w)| w.stealer())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let core = Arc::new(ThreadPoolCore {
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
                                        /* index, */
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

        Self(core, threads)
    }
}

impl<J> ThreadPool<J> {
    /// Push `job` onto the end of the work queue for this thread pool.
    #[inline]
    pub fn push(&self, job: J) { self.0.push(job); }

    fn join_threads(&mut self) {
        for handle in self.1.drain(..) {
            handle.join().unwrap();
        }
    }

    /// Wait for all currently-running jobs (and any jobs queued as a result of
    /// currently-running jobs) to finish processing and terminate all
    /// associated threads.
    ///
    /// # Panics
    /// This function panics if any of the threads in this pool have panicked
    /// without already aborting the process.
    pub fn join(mut self) {
        self.0.join();
        self.join_threads();

        // Final sanity check
        assert!(self.0.is_empty(), "Thread pool starved!");
    }

    /// Stop processing new jobs and terminate the associated threads after all
    /// current jobs are done.
    ///
    /// # Panics
    /// This function panics if any of the threads in this pool have panicked
    /// without already aborting the process.
    #[inline]
    pub fn abort(mut self) {
        self.0.abort();
        self.join_threads();
    }
}

impl<J> Drop for ThreadPool<J> {
    fn drop(&mut self) { self.0.abort(); }
}

struct WorkerThread<J> {
    // Might expose in the future
    // index: usize,
    work: Worker<J>,
    core: Arc<ThreadPoolCore<J>>,
}

impl<J: UnwindSafe> WorkerThread<J> {
    fn get_job(&self) -> Option<J> {
        self.work.pop().or_else(|| {
            let WorkerThread { work, .. } = self;
            let ThreadPoolCore {
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

    fn run(self, f: impl Fn(J, ThreadPoolHandle<J>) + RefUnwindSafe) {
        abort_on_panic(|| {
            while !self.core.stop.load(Ordering::Acquire) {
                if let Some(job) = self.get_job() {
                    let core_ref = AssertUnwindSafe(&self.core);
                    match std::panic::catch_unwind(|| f(job, ThreadPoolHandle(core_ref))) {
                        Ok(()) => (),
                        Err(e) => log::error!("Job panicked: {:?}", e),
                    }
                } else {
                    self.core.park();
                }
            }
        })
    }
}
