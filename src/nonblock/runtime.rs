use std::{future::Future, marker::PhantomData};

pub trait Runtime {
    type SpawnError: std::fmt::Debug;

    fn spawn<F: Future>(
        &self,
        name: String,
        fut: F,
    ) -> Result<JoinHandle<F::Output>, Self::SpawnError>;
}

#[derive(Debug)]
pub struct JoinHandle<T>(PhantomData<fn() -> T>);

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, Box<dyn std::any::Any + Send + 'static>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        todo!()
    }
}

