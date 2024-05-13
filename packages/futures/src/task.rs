use dashmap::DashMap;
use futures::{
	future::{self, BoxFuture},
	Future, FutureExt as _, TryFutureExt as _,
};
use std::{
	hash::{BuildHasher, Hash},
	sync::Arc,
};

#[derive(Clone)]
pub struct Task<T> {
	abort: Arc<tokio::task::AbortHandle>,
	stop: Stop,
	future: future::Shared<BoxFuture<'static, T>>,
}

#[allow(clippy::module_name_repetitions)]
pub struct TaskMap<K, T, H> {
	map: Arc<DashMap<K, Task<T>, H>>,
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
	K: Hash + Eq + Clone + Send + Sync + 'static,
	T: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	pub fn spawn(&self, key: K, task: Task<T>) {
		self.map.insert(key, task);
	}

	pub fn get_or_spawn<F, Fut>(&self, key: K, f: F) -> Task<T>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let map = self.map.clone();
		self.map
			.entry(key.clone())
			.or_insert_with(move || {
				Task::spawn(move |stop| {
					f(stop).inspect(move |_| {
						map.remove(&key);
					})
				})
			})
			.value()
			.clone()
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
	K: Hash + Eq + Clone + Send + Sync + 'static,
	T: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	fn default() -> Self {
		let map = Arc::new(DashMap::default());
		Self { map }
	}
}

impl Default for Stop {
	fn default() -> Self {
		Self::new()
	}
}
