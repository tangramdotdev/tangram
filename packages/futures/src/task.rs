use dashmap::DashMap;
use futures::{
	future::{self, BoxFuture},
	Future, FutureExt as _, TryFutureExt as _,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Task<T> {
	abort: Arc<tokio::task::AbortHandle>,
	stop: Stop,
	future: future::Shared<BoxFuture<'static, T>>,
}

#[allow(clippy::module_name_repetitions)]
pub struct TaskMap<K, T, H> {
	map: DashMap<K, Task<T>, H>,
}

#[derive(Clone)]
pub struct Stop(tokio::sync::watch::Sender<bool>);

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
		let future = task.map(Result::unwrap).boxed().shared();
		Self {
			abort,
			stop,
			future,
		}
	}

	pub fn abort(&self) {
		self.abort.abort();
	}

	pub fn stop(&self) {
		self.stop.stop();
	}

	pub async fn wait(&self) -> T {
		self.future.clone().await
	}
}

impl<K, T, H> TaskMap<K, T, H>
where
	K: std::hash::Hash + Eq,
	T: Clone + Send + 'static,
	H: std::hash::BuildHasher + Default + Clone,
{
	pub fn spawn(&self, key: K, task: Task<T>) {
		self.map.insert(key, task);
	}

	pub fn get_or_spawn(&self, key: K, f: impl FnOnce() -> Task<T>) -> Task<T> {
		self.map.entry(key).or_insert_with(f).value().clone()
	}

	pub fn abort(&self, key: &K) {
		if let Some(task) = self.map.get(key) {
			task.abort();
		}
	}

	pub fn stop(&self, key: &K) {
		if let Some(task) = self.map.get(key) {
			task.stop();
		}
	}
}

impl Stop {
	#[must_use]
	pub fn new() -> Self {
		let (sender, _) = tokio::sync::watch::channel(false);
		Self(sender)
	}

	pub fn stop(&self) {
		self.0.send_replace(true);
	}

	#[must_use]
	pub fn is_stopped(&self) -> bool {
		*self.0.subscribe().borrow()
	}

	pub async fn stopped(&self) {
		self.0
			.subscribe()
			.wait_for(|stop| *stop)
			.map_ok(|_| ())
			.await
			.unwrap();
	}
}

impl<K, T, H> Default for TaskMap<K, T, H>
where
	K: std::hash::Hash + Eq,
	H: std::hash::BuildHasher + Default + Clone,
{
	fn default() -> Self {
		let map = DashMap::default();
		Self { map }
	}
}

impl Default for Stop {
	fn default() -> Self {
		Self::new()
	}
}
