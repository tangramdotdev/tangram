use {
	crate::task::Stop,
	futures::{future::BoxFuture, prelude::*},
	std::sync::{Arc, atomic::AtomicBool},
};

#[derive(Clone)]
pub struct Task<T, C = ()>(Arc<Inner<T, C>>);

struct Inner<T, C> {
	abort_on_drop: AtomicBool,
	abort_handle: tokio::task::AbortHandle,
	context: C,
	future: future::Shared<BoxFuture<'static, Result<T, Arc<tokio::task::JoinError>>>>,
	stop: Stop,
}

impl<T, C> Task<T, C>
where
	T: Clone + Send + 'static,
	C: Clone + Send + Sync + 'static,
{
	pub fn spawn_with_context<F, Fut>(context: C, f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let stop = Stop::new();
		let task = tokio::spawn(f(stop.clone()));
		let abort_handle = task.abort_handle();
		let future = task.map_err(Arc::new).boxed().shared();
		Self(Arc::new(Inner {
			abort_handle,
			abort_on_drop: AtomicBool::new(true),
			context,
			future,
			stop,
		}))
	}

	pub fn spawn_local_with_context<F, Fut>(context: C, f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + 'static,
	{
		let stop = Stop::new();
		let task = tokio::task::spawn_local(f(stop.clone()));
		let abort_handle = task.abort_handle();
		let future = task.map_err(Arc::new).boxed().shared();
		Self(Arc::new(Inner {
			abort_handle,
			abort_on_drop: AtomicBool::new(true),
			context,
			future,
			stop,
		}))
	}

	pub fn spawn_blocking_with_context<F>(context: C, f: F) -> Self
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
			abort_handle,
			abort_on_drop: AtomicBool::new(true),
			context,
			future,
			stop,
		}))
	}

	#[must_use]
	pub fn context(&self) -> &C {
		&self.0.context
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

impl<T> Task<T, ()>
where
	T: Clone + Send + 'static,
{
	pub fn spawn<F, Fut>(f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		Self::spawn_with_context((), f)
	}

	pub fn spawn_local<F, Fut>(f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + 'static,
	{
		Self::spawn_local_with_context((), f)
	}

	pub fn spawn_blocking<F>(f: F) -> Self
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		Self::spawn_blocking_with_context((), f)
	}
}

impl<T, C> Drop for Inner<T, C> {
	fn drop(&mut self) {
		if self.abort_on_drop.load(std::sync::atomic::Ordering::SeqCst) {
			self.abort_handle.abort();
		}
	}
}
