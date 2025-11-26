use {
	crate::task::Stop,
	futures::{future::BoxFuture, prelude::*},
	std::sync::{Arc, atomic::AtomicBool},
};

#[derive(Clone)]
pub struct Task<T>(Arc<Inner<T>>);

struct Inner<T> {
	abort_on_drop: AtomicBool,
	abort_handle: tokio::task::AbortHandle,
	future: future::Shared<BoxFuture<'static, Result<T, Arc<tokio::task::JoinError>>>>,
	stop: Stop,
}

impl<T> Task<T>
where
	T: Clone + Send + 'static,
{
	pub fn spawn<F, Fut>(f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let stop = Stop::new();
		let task = tokio::spawn(f(stop.clone()));
		let abort_handle = task.abort_handle();
		let future = task.map_err(Arc::new).boxed().shared();
		Self(Arc::new(Inner {
			abort_on_drop: AtomicBool::new(true),
			abort_handle,
			future,
			stop,
		}))
	}

	pub fn spawn_local<F, Fut>(f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + 'static,
	{
		let stop = Stop::new();
		let task = tokio::task::spawn_local(f(stop.clone()));
		let abort_handle = task.abort_handle();
		let future = task.map_err(Arc::new).boxed().shared();
		Self(Arc::new(Inner {
			abort_on_drop: AtomicBool::new(true),
			abort_handle,
			future,
			stop,
		}))
	}

	pub fn spawn_blocking<F>(f: F) -> Self
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let stop = Stop::new();
		let task = tokio::task::spawn_blocking({
			let stop = stop.clone();
			move || f(stop)
		});
		let abort_handle = task.abort_handle();
		let future = task.map_err(Arc::new).boxed().shared();
		Self(Arc::new(Inner {
			abort_on_drop: AtomicBool::new(true),
			abort_handle,
			future,
			stop,
		}))
	}

	#[must_use]
	pub fn id(&self) -> tokio::task::Id {
		self.0.abort_handle.id()
	}

	pub fn abort(&self) {
		self.0.abort_handle.abort();
	}

	pub fn detach(&self) {
		self.0
			.abort_on_drop
			.store(false, std::sync::atomic::Ordering::SeqCst);
	}

	pub fn stop(&self) {
		self.0.stop.stop();
	}

	pub async fn wait(&self) -> Result<T, Arc<tokio::task::JoinError>> {
		self.0.future.clone().await
	}
}

impl<T> Drop for Inner<T> {
	fn drop(&mut self) {
		if self.abort_on_drop.load(std::sync::atomic::Ordering::SeqCst) {
			self.abort_handle.abort();
		}
	}
}
