use {
	crate::task::{Shared, Stop},
	dashmap::DashMap,
	futures::{prelude::*, stream::FuturesUnordered},
	std::{
		collections::hash_map::RandomState,
		hash::{BuildHasher, Hash},
		sync::Arc,
	},
};

pub struct Map<K, T, C = (), H = RandomState> {
	map: Arc<DashMap<K, Shared<T, C>, H>>,
}

impl<K, T, C, H> Map<K, T, C, H>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	T: Clone + Send + Sync + 'static,
	C: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	pub fn spawn_with_context<G, F, Fut>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let map = self.map.clone();
		let context = context();
		let value = Shared::spawn_with_context(context.clone(), {
			let key = key.clone();
			move |stop| {
				let future = f(context, stop);
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

	pub fn get_or_spawn_with_context<G, F, Fut>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let map = self.map.clone();
		self.map
			.entry(key.clone())
			.or_insert_with(move || {
				let context = context();
				Shared::spawn_with_context(context.clone(), move |stop| {
					let future = f(context, stop);
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

	pub fn get_or_spawn_blocking_with_context<F>(&self, key: K, context: C, f: F) -> Shared<T, C>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let map = self.map.clone();
		self.map
			.entry(key.clone())
			.or_insert_with(move || {
				Shared::spawn_blocking_with_context(context, move |stop| {
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

impl<K, T, H> Map<K, T, (), H>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	T: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	pub fn spawn<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn get_or_spawn<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.get_or_spawn_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn get_or_spawn_blocking<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		self.get_or_spawn_blocking_with_context(key, (), f)
	}
}

impl<K, T, C, H> Clone for Map<K, T, C, H> {
	fn clone(&self) -> Self {
		Self {
			map: self.map.clone(),
		}
	}
}

impl<K, T, C, H> Default for Map<K, T, C, H>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	T: Clone + Send + Sync + 'static,
	C: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	fn default() -> Self {
		let map = Arc::new(DashMap::default());
		Self { map }
	}
}
