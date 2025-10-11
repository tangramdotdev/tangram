use {
	crate::task::Stop,
	futures::{future::BoxFuture, prelude::*},
	std::sync::Arc,
};

#[derive(Clone)]
pub struct Task<T> {
	abort: Arc<tokio::task::AbortHandle>,
	stop: Stop,
	future: future::Shared<BoxFuture<'static, Result<T, Arc<tokio::task::JoinError>>>>,
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
		let abort = Arc::new(task.abort_handle());
		let future = task.map_err(Arc::new).boxed().shared();
		Self {
			abort,
			stop,
			future,
		}
	}

	pub fn spawn_local<F, Fut>(f: F) -> Self
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + 'static,
	{
		let stop = Stop::new();
		let task = tokio::task::spawn_local(f(stop.clone()));
		let abort = Arc::new(task.abort_handle());
		let future = task.map_err(Arc::new).boxed().shared();
		Self {
			abort,
			stop,
			future,
		}
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
		let abort = Arc::new(task.abort_handle());
		let future = task.map_err(Arc::new).boxed().shared();
		Self {
			abort,
			stop,
			future,
		}
	}

	#[must_use]
	pub fn id(&self) -> tokio::task::Id {
		self.abort.id()
	}

	pub fn abort(&self) {
		self.abort.abort();
	}

	pub fn stop(&self) {
		self.stop.stop();
	}

	pub async fn wait(&self) -> Result<T, Arc<tokio::task::JoinError>> {
		self.future.clone().await
	}
}
