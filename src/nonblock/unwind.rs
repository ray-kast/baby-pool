use std::{
    pin::{pin, Pin},
    task::{Context, Poll},
};

use dispose::abort_on_panic;
use futures_util::Future;
use pin_project::pin_project;

#[pin_project]
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct AbortOnPanic<F>(#[pin] pub F);

impl<F: Future> Future for AbortOnPanic<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        abort_on_panic(|| self.project().0.poll(cx))
    }
}
