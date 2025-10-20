use {std::future::Future, tokio_util::task::AbortOnDropHandle};

pub use self::{map::Map, set::Set, shared::Task as Shared, stop::Stop};

pub mod map;
pub mod set;
pub mod shared;
pub mod stop;

pub struct Task<T> {
	stop: Stop,
	handle: AbortOnDropHandle<T>,
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
		let handle = AbortOnDropHandle::new(tokio::spawn(f(stop.clone())));
		Self { stop, handle }
	}

	pub fn spawn_local<F, Fut>(f: F) -> Self
	where
		F: 'static,
		Fut: Future<Output = T> + 'static,
		F: FnOnce(Stop) -> Fut,
	{
		let stop = Stop::new();
		let handle = AbortOnDropHandle::new(tokio::task::spawn_local(f(stop.clone())));
		Self { stop, handle }
	}

	pub fn spawn_blocking<F>(f: F) -> Self
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let stop = Stop::new();
		let handle = AbortOnDropHandle::new(tokio::task::spawn_blocking({
			let stop = stop.clone();
			move || f(stop)
		}));
		Self { stop, handle }
	}

	pub fn abort(&self) {
		self.handle.abort();
	}

	pub fn stop(&self) {
		self.stop.stop();
	}

	pub async fn wait(self) -> Result<T, tokio::task::JoinError> {
		self.handle.await
	}
}
