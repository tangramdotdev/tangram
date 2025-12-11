use {futures::FutureExt as _, tokio_util::task::AbortOnDropHandle};

pub use self::{map::Map, set::Set, shared::Task as Shared, stop::Stop};

pub mod map;
pub mod set;
pub mod shared;
pub mod stop;

pub struct Task<T, C = ()> {
	attached: bool,
	context: C,
	handle: Option<tokio::task::JoinHandle<T>>,
	stop: Stop,
}

impl<T, C> Task<T, C>
where
	T: Send + 'static,
	C: Clone + Send + Sync + 'static,
{
	pub fn spawn_with_context<F, Fut>(context: C, f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let stop = Stop::new();
		let handle = tokio::spawn(f(stop.clone()));
		Self {
			attached: true,
			context,
			stop,
			handle: Some(handle),
		}
	}

	pub fn spawn_local_with_context<F, Fut>(context: C, f: F) -> Self
	where
		F: 'static,
		Fut: Future<Output = T> + 'static,
		F: FnOnce(Stop) -> Fut,
	{
		let stop = Stop::new();
		let handle = tokio::task::spawn_local(f(stop.clone()));
		Self {
			attached: true,
			context,
			stop,
			handle: Some(handle),
		}
	}

	pub fn spawn_blocking_with_context<F>(context: C, f: F) -> Self
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let stop = Stop::new();
		let handle = tokio::task::spawn_blocking({
			let stop = stop.clone();
			move || f(stop)
		});
		Self {
			attached: true,
			context,
			stop,
			handle: Some(handle),
		}
	}

	#[must_use]
	pub fn context(&self) -> &C {
		&self.context
	}

	#[must_use]
	pub fn id(&self) -> tokio::task::Id {
		self.handle.as_ref().unwrap().id()
	}

	pub fn abort(&self) {
		self.handle.as_ref().unwrap().abort();
	}

	#[must_use]
	pub fn attached(&self) -> bool {
		self.attached
	}

	pub fn attach(&mut self) {
		self.attached = true;
	}

	pub fn detach(&mut self) {
		self.attached = false;
	}

	pub fn stop(&self) {
		self.stop.stop();
	}

	pub fn wait(mut self) -> impl Future<Output = Result<T, tokio::task::JoinError>> {
		let handle = self.handle.take().unwrap();
		if self.attached {
			AbortOnDropHandle::new(handle).left_future()
		} else {
			handle.right_future()
		}
	}
}

impl<T> Task<T, ()>
where
	T: Send + 'static,
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
		F: 'static,
		Fut: Future<Output = T> + 'static,
		F: FnOnce(Stop) -> Fut,
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

impl<T, C> Drop for Task<T, C> {
	fn drop(&mut self) {
		if self.attached
			&& let Some(handle) = self.handle.take()
		{
			handle.abort();
		}
	}
}
