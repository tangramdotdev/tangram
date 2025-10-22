use {
	crate::task::{Shared, Stop},
	dashmap::DashMap,
	futures::{prelude::*, stream::FuturesUnordered},
	std::{
		hash::{BuildHasher, Hash},
		sync::Arc,
	},
};

pub struct Map<K, T, H> {
	map: Arc<DashMap<K, Shared<T>, H>>,
}

impl<K, T, H> Map<K, T, H>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	T: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	pub fn spawn<F, Fut>(&self, key: K, f: F) -> Shared<T>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let map = self.map.clone();
		let value = Shared::spawn({
			let key = key.clone();
			move |stop| {
				let future = f(stop);
				async move {
					let result = future.await;
					let id = tokio::task::id();
					map.remove_if(&key, |_, task| task.id() == id);
					result
				}
			}
		});
		self.map.insert(key, value.clone());
		value
	}

	pub fn get_or_spawn<F, Fut>(&self, key: K, f: F) -> Shared<T>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let map = self.map.clone();
		self.map
			.entry(key.clone())
			.or_insert_with(move || {
				Shared::spawn(move |stop| {
					let future = f(stop);
					async move {
						let result = future.await;
						let id = tokio::task::id();
						map.remove_if(&key, |_, task| task.id() == id);
						result
					}
				})
			})
			.value()
			.clone()
	}

	pub fn get_or_spawn_blocking<F>(&self, key: K, f: F) -> Shared<T>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let map = self.map.clone();
		self.map
			.entry(key.clone())
			.or_insert_with(move || {
				Shared::spawn_blocking(move |stop| {
					let result = f(stop);
					let id = tokio::task::id();
					map.remove_if(&key, |_, task| task.id() == id);
					result
				})
			})
			.value()
			.clone()
	}

	pub fn get_task_id(&self, key: &K) -> Option<tokio::task::Id> {
		self.map.get(key).map(|task| task.id())
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
			.collect::<Vec<_>>();
		tasks
			.into_iter()
			.map(|task| async move { task.wait().await })
			.collect::<FuturesUnordered<_>>()
			.collect()
			.await
	}
}

impl<K, T, H> Clone for Map<K, T, H> {
	fn clone(&self) -> Self {
		Self {
			map: self.map.clone(),
		}
	}
}

impl<K, T, H> Default for Map<K, T, H>
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
