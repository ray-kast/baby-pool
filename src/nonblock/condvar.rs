#[cfg(debug_assertions)]
use std::sync::atomic::AtomicBool;
use std::{
    cell::{OnceCell, UnsafeCell},
    fmt,
    future::Future,
    pin::Pin,
    ptr::{self, NonNull},
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc, Weak,
    },
    task::{Context, Poll, Waker},
};

use crossbeam::queue::SegQueue;
use parking_lot::lock_api::RawMutex as _;

#[derive(Debug)]
pub struct Unpark(AtomicPtr<Waker>);

impl Unpark {
    const UNPARKED: *mut Waker = NonNull::dangling().as_ptr();
    const UNPOLLED: *mut Waker = ptr::null_mut();

    #[inline]
    const fn new() -> Self { Self(AtomicPtr::new(Self::UNPOLLED)) }

    unsafe fn get_waker(ptr: *mut Waker) -> Option<Box<Waker>> {
        match ptr {
            Self::UNPOLLED | Self::UNPARKED => None,
            w => Some(Box::from_raw(w)),
        }
    }

    fn box_waker(w: Box<Waker>) -> *mut Waker { Box::into_raw(w) }

    #[inline]
    pub fn unpark(&self) {
        let ptr = self.0.swap(Self::UNPARKED, Ordering::SeqCst);
        if let Some(waker) = unsafe { Self::get_waker(ptr) } {
            waker.wake();
        }
    }
}

impl Drop for Unpark {
    fn drop(&mut self) {
        let ptr = self.0.swap(Self::UNPARKED, Ordering::SeqCst);
        if let Some(waker) = unsafe { Self::get_waker(ptr) } {
            drop(waker);
        }
    }
}

#[derive(Debug)]
pub struct Park(Arc<Unpark>);

impl Future for Park {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = OnceCell::new();
        let prev = self
            .0
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |p| {
                (p == Unpark::UNPOLLED)
                    .then(|| *waker.get_or_init(|| Unpark::box_waker(cx.waker().clone().into())))
            });
        match prev {
            Ok(p) => {
                debug_assert_eq!(p, Unpark::UNPOLLED);
                Poll::Pending
            },
            Err(Unpark::UNPARKED) => Poll::Ready(()),
            Err(p) => {
                unreachable!("Invalid unpark pointer {p:x?} encountered, this should not happen")
            },
        }
    }
}

#[must_use = "The future returned by park() must be awaited"]
pub fn park() -> (Park, Weak<Unpark>) {
    let unpark = Arc::new(Unpark::new());
    let unpark_weak = Arc::downgrade(&unpark);

    (Park(unpark), unpark_weak)
}

#[derive(Debug)]
#[repr(transparent)]
pub struct WaitQueue(SegQueue<Weak<Unpark>>);

impl Default for WaitQueue {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl WaitQueue {
    #[inline]
    #[must_use]
    pub const fn new() -> Self { Self(SegQueue::new()) }

    pub fn push(&self) -> Park {
        let (park, unpark) = park();
        self.0.push(unpark);
        park
    }

    pub fn pop(&self) -> bool {
        loop {
            let Some(unpark) = self.0.pop() else {
                break false;
            };
            let Some(unpark) = unpark.upgrade() else {
                continue;
            };
            unpark.unpark();
            break true;
        }
    }

    pub fn clear(&self) -> usize {
        let mut n = 0;
        for _ in 0..self.0.len() {
            let Some(unpark) = self.0.pop() else { break };
            let Some(unpark) = unpark.upgrade() else {
                continue;
            };
            unpark.unpark();
            n += 1;
        }

        n
    }
}

#[cfg_attr(not(debug_assertions), repr(transparent))]
struct RawMutex(parking_lot::RawMutex, #[cfg(debug_assertions)] AtomicBool);

impl RawMutex {
    #[cfg(debug_assertions)]
    #[inline]
    const fn new() -> Self { Self(parking_lot::RawMutex::INIT, AtomicBool::new(false)) }

    #[cfg(not(debug_assertions))]
    #[inline]
    const fn new() -> Self { Self(parking_lot::RawMutex::INIT) }

    #[cfg(debug_assertions)]
    fn try_lock(&self) -> bool {
        if self.0.try_lock() {
            self.1
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .unwrap_or_else(|_| panic!("Lock-after-lock on RawMutex"));
            true
        } else {
            false
        }
    }

    #[cfg(not(debug_assertions))]
    fn try_lock(&self) -> bool { self.0.try_lock() }

    unsafe fn unlock(&self) {
        #[cfg(debug_assertions)]
        {
            self.1
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .unwrap_or_else(|_| panic!("Unlock-after-unlock on RawMutex"));
        }

        self.0.unlock();
    }
}

pub struct Mutex<T: ?Sized> {
    queue: WaitQueue,
    raw: RawMutex,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send + ?Sized> Send for Mutex<T> {}
unsafe impl<T: Send + ?Sized> Sync for Mutex<T> {}

#[derive(Debug)]
pub struct MutexGuard<'a, T: ?Sized>(Option<&'a Mutex<T>>);

impl<T: fmt::Debug + ?Sized> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Locked;

        impl fmt::Debug for Locked {
            #[inline]
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("<locked>") }
        }

        let mut f = f.debug_tuple("Mutex");

        if self.raw.try_lock() {
            // SAFETY: the mutex has been locked
            unsafe {
                f.field(&&*self.value.get());
                self.raw.unlock();
            }
        } else {
            f.field(&Locked);
        }

        f.finish()
    }
}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Self {
            queue: WaitQueue::new(),
            raw: RawMutex::new(),
            value: value.into(),
        }
    }
}

impl<T: ?Sized> Mutex<T> {
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        loop {
            if self.raw.try_lock() {
                break;
            }

            self.queue.push().await;
        }

        MutexGuard(Some(self))
    }
}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    fn mutex(&self) -> &'a Mutex<T> {
        self.0
            .expect("Attempt to access bumped/poisoned mutex guard")
    }

    #[inline]
    pub fn unlock(self) { drop(self); }
}

impl<'a, T: ?Sized> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        if let Some(mutex) = self.0 {
            // SAFETY: the mutex must have been locked by the call to lock that
            //         created this guard
            unsafe { mutex.raw.unlock() }
            mutex.queue.pop();
        }
    }
}

impl<'a, T> std::ops::Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: the mutex has been locked if the guard is live
        unsafe { &*self.mutex().value.get() }
    }
}

impl<'a, T> std::ops::DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: the mutex has been locked if the guard is live
        unsafe { &mut *self.mutex().value.get() }
    }
}

#[derive(Debug)]
pub struct Condvar(WaitQueue);

impl Default for Condvar {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl Condvar {
    #[inline]
    #[must_use]
    pub const fn new() -> Self { Self(WaitQueue::new()) }

    #[inline]
    pub async fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
        let mutex = guard
            .0
            .take()
            .expect("Attempt to wait on condition variable with bumped/poisoned mutex guard");
        // SAFETY: we now hold the exclusive guard reference for the mutex
        unsafe {
            mutex.raw.unlock();
        }

        self.0.push().await;
        *guard = mutex.lock().await;
    }

    #[inline]
    pub fn notify_one(&self) -> bool { self.0.pop() }

    #[inline]
    pub fn notify_all(&self) -> usize { self.0.clear() }
}
