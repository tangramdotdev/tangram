use {futures::FutureExt as _, tokio_util::task::AbortOnDropHandle};

pub use self::{map::Map, set::Set, shared::Task as Shared, stop::Stop};

pub mod map;
pub mod set;
pub mod shared;
pub mod stop;

pub struct Task<T> {
	abort_on_drop: bool,
	handle: Option<tokio::task::JoinHandle<T>>,
	stop: Stop,
}

impl<T> Task<T>
where
	T: Send + 'static,
{
	pub fn spawn<F, Fut>(f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let stop = Stop::new();
		let handle = tokio::spawn(f(stop.clone()));
		Self {
			abort_on_drop: true,
			stop,
			handle: Some(handle),
		}
	}

	pub fn spawn_local<F, Fut>(f: F) -> Self
	where
		F: 'static,
		Fut: Future<Output = T> + 'static,
		F: FnOnce(Stop) -> Fut,
	{
		let stop = Stop::new();
		let handle = tokio::task::spawn_local(f(stop.clone()));
		Self {
			abort_on_drop: true,
			stop,
			handle: Some(handle),
		}
	}

	pub fn spawn_blocking<F>(f: F) -> Self
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let stop = Stop::new();
		let handle = tokio::task::spawn_blocking({
			let stop = stop.clone();
			move || f(stop)
		});
		Self {
			abort_on_drop: true,
			stop,
			handle: Some(handle),
		}
	}

	pub fn abort(&self) {
		self.handle.as_ref().unwrap().abort();
	}

	pub fn detach(&mut self) {
		self.abort_on_drop = false;
	}

	pub fn stop(&self) {
		self.stop.stop();
	}

	pub fn wait(mut self) -> impl Future<Output = Result<T, tokio::task::JoinError>> {
		let handle = self.handle.take().unwrap();
		if self.abort_on_drop {
			AbortOnDropHandle::new(handle).left_future()
		} else {
			handle.right_future()
		}
	}
}

impl<T> Drop for Task<T> {
	fn drop(&mut self) {
		if self.abort_on_drop
			&& let Some(handle) = self.handle.take()
		{
			handle.abort();
		}
	}
}
