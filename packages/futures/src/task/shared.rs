use {
	crate::task::Stop,
	futures::{future::BoxFuture, prelude::*},
	std::sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
};

pub struct Task<T, C = ()> {
	inner: Arc<Inner<T, C>>,
	attached: bool,
}

struct Inner<T, C> {
	abort_handle: tokio::task::AbortHandle,
	attached_count: AtomicUsize,
	context: C,
	future: future::Shared<BoxFuture<'static, Result<T, Arc<tokio::task::JoinError>>>>,
	on_drop: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
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
		let inner = Inner {
			abort_handle,
			attached_count: AtomicUsize::new(1),
			context,
			future,
			on_drop: Mutex::new(None),
			stop,
		};
		Self {
			inner: Arc::new(inner),
			attached: true,
		}
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
		let inner = Inner {
			abort_handle,
			attached_count: AtomicUsize::new(1),
			context,
			future,
			on_drop: Mutex::new(None),
			stop,
		};
		Self {
			inner: Arc::new(inner),
			attached: true,
		}
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
		let inner = Inner {
			abort_handle,
			attached_count: AtomicUsize::new(1),
			context,
			future,
			on_drop: Mutex::new(None),
			stop,
		};
		Self {
			inner: Arc::new(inner),
			attached: true,
		}
	}

	#[must_use]
	pub fn context(&self) -> &C {
		&self.inner.context
	}

	#[must_use]
	pub fn id(&self) -> tokio::task::Id {
		self.inner.abort_handle.id()
	}

	pub fn abort(&self) {
		self.inner.abort_handle.abort();
	}

	#[must_use]
	pub fn attached(&self) -> bool {
		self.attached
	}

	pub fn attach(&mut self) {
		if !self.attached {
			self.attached = true;
			self.inner.attached_count.fetch_add(1, Ordering::AcqRel);
		}
	}

	pub fn detach(&mut self) {
		if self.attached {
			self.attached = false;
			self.inner.attached_count.fetch_sub(1, Ordering::AcqRel);
		}
	}

	pub fn set_on_drop<F>(&self, f: F)
	where
		F: FnOnce() + Send + Sync + 'static,
	{
		*self.inner.on_drop.lock().unwrap() = Some(Box::new(f));
	}

	pub fn stop(&self) {
		self.inner.stop.stop();
	}

	pub async fn wait(&self) -> Result<T, Arc<tokio::task::JoinError>> {
		self.inner.future.clone().await
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

impl<T, C> Clone for Task<T, C> {
	fn clone(&self) -> Self {
		if self.attached {
			self.inner.attached_count.fetch_add(1, Ordering::AcqRel);
		}
		Self {
			inner: self.inner.clone(),
			attached: self.attached,
		}
	}
}

impl<T, C> Drop for Task<T, C> {
	fn drop(&mut self) {
		if self.attached {
			let prev = self.inner.attached_count.fetch_sub(1, Ordering::AcqRel);
			if prev == 1 {
				self.inner.abort_handle.abort();
				if let Some(callback) = self.inner.on_drop.lock().unwrap().take() {
					callback();
				}
			}
		}
	}
}
