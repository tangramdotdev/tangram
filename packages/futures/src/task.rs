use dashmap::DashMap;
use futures::{
	FutureExt as _, StreamExt as _, TryFutureExt as _,
	future::{self, BoxFuture},
	stream::FuturesUnordered,
};
use itertools::Itertools as _;
use std::{
	hash::{BuildHasher, Hash},
	sync::Arc,
};

#[derive(Clone)]
pub struct Task<T> {
	abort: Arc<tokio::task::AbortHandle>,
	stop: Stop,
	future: future::Shared<BoxFuture<'static, Result<T, Arc<tokio::task::JoinError>>>>,
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

	pub fn get_or_spawn_blocking<F>(&self, key: K, f: F) -> Task<T>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let map = self.map.clone();
		self.map
			.entry(key.clone())
			.or_insert_with(move || {
				Task::spawn_blocking(move |stop| {
					let output = f(stop);
					map.remove(&key);
					output
				})
			})
			.value()
			.clone()
	}

	pub fn get_task_id(&self, key: &K) -> Option<tokio::task::Id> {
		self.map.get(key).map(|task| task.abort.id())
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

	pub fn abort_all(&self) {
		for task in self.map.iter() {
			task.abort();
		}
	}

	pub fn stop_all(&self) {
		for task in self.map.iter() {
			task.stop();
		}
	}

	pub async fn wait(&self) -> Vec<Result<T, Arc<tokio::task::JoinError>>> {
		let tasks = self
			.map
			.iter()
			.map(|entry| entry.value().clone())
			.collect_vec();
		tasks
			.into_iter()
			.map(|task| async move { task.wait().await })
			.collect::<FuturesUnordered<_>>()
			.collect()
			.await
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
	pub fn stopped(&self) -> bool {
		*self.0.subscribe().borrow()
	}

	pub fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
		let sender = self.0.clone();
		async move {
			sender
				.subscribe()
				.wait_for(|stop| *stop)
				.map_ok(|_| ())
				.await
				.unwrap();
		}
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
