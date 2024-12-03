use bytes::Bytes;
use futures::{
	future::{self, BoxFuture},
	FutureExt, TryFutureExt as _,
};
use pin_project::pin_project;
use std::{
	pin::pin,
	sync::{
		atomic::{AtomicUsize, Ordering},
		Arc,
	},
	time::Duration,
};

use crate::Error;

#[pin_project]
pub struct Body<T: hyper::body::Body<Data = Bytes, Error = Error>> {
	#[pin]
	body: T,
	token: Token,
}

impl<T: hyper::body::Body<Data = Bytes, Error = Error>> Body<T> {
	#[must_use]
	pub fn new(token: Token, body: T) -> Self
	where
		T: hyper::body::Body,
	{
		Self { body, token }
	}
}

impl<T: hyper::body::Body<Data = Bytes, Error = Error>> hyper::body::Body for Body<T> {
	type Data = Bytes;

	type Error = Error;

	fn poll_frame(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
		let this = self.project();
		this.body.poll_frame(cx)
	}
}

pub struct Token {
	idle: Idle,
}

impl Drop for Token {
	fn drop(&mut self) {
		self.idle.inner.drop_token();
	}
}

#[derive(Clone)]
pub struct Idle {
	inner: Arc<Inner>,
}

impl Idle {
	#[must_use]
	pub fn new(duration: Duration) -> Self {
		Self {
			inner: Arc::new(Inner::new(duration)),
		}
	}

	pub async fn wait(&self) -> Result<(), Arc<tokio::task::JoinError>> {
		self.inner.wait().await
	}

	#[must_use]
	pub fn token(&self) -> Token {
		self.inner.new_token();
		Token { idle: self.clone() }
	}
}

pub struct Inner {
	abort: Arc<tokio::task::AbortHandle>,
	count: AtomicUsize,
	duration: Duration,
	future: future::Shared<BoxFuture<'static, Result<(), Arc<tokio::task::JoinError>>>>,
	tx: tokio::sync::watch::Sender<Option<Duration>>,
}

impl Inner {
	#[must_use]
	fn new(duration: Duration) -> Self {
		let (tx, mut rx) = tokio::sync::watch::channel(None);
		let task = tokio::spawn({
			async move {
				loop {
					let sleep = if let Some(duration) = *rx.borrow_and_update() {
						tokio::time::sleep(duration).boxed()
					} else {
						future::pending().boxed()
					};
					match future::select(pin!(sleep), pin!(rx.clone().changed())).await {
						future::Either::Left(_) => break,
						future::Either::Right((result, _)) => {
							if result.is_err() {
								break;
							}
							continue;
						},
					}
				}
			}
		});
		let abort = Arc::new(task.abort_handle());
		let future = task.map_err(Arc::new).boxed().shared();
		let count = AtomicUsize::new(0);

		Self {
			abort,
			count,
			duration,
			future,
			tx,
		}
	}

	async fn wait(&self) -> Result<(), Arc<tokio::task::JoinError>> {
		self.future.clone().await
	}

	fn abort(&self) {
		self.abort.abort();
	}

	fn new_token(&self) {
		let count = self.count.fetch_add(1, Ordering::Relaxed);
		if count == 0 {
			let _ = self.tx.send(None);
		}
	}

	fn drop_token(&self) {
		let count = self.count.fetch_sub(1, Ordering::Release);
		if count == 1 {
			let _ = self.tx.send(Some(self.duration));
		}
	}
}

impl Drop for Inner {
	fn drop(&mut self) {
		self.abort();
	}
}
