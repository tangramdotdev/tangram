use {
	crate::task::{Shared, Stop},
	dashmap::DashMap,
	futures::{prelude::*, stream::FuturesUnordered},
	std::{
		collections::hash_map::RandomState,
		hash::{BuildHasher, Hash},
		panic::{AssertUnwindSafe, catch_unwind},
		sync::Arc,
	},
};

pub struct Map<K, T, C = (), H = RandomState>(Arc<DashMap<K, Shared<T, C>, H>>);

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
		self.spawn_inner(key, context, f, false, false)
	}

	pub fn spawn_detached_with_context<G, F, Fut>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_inner(key, context, f, false, true)
	}

	pub fn get_or_spawn_with_context<G, F, Fut>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_inner(key, context, f, true, false)
	}

	pub fn get_or_spawn_detached_with_context<G, F, Fut>(
		&self,
		key: K,
		context: G,
		f: F,
	) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_inner(key, context, f, true, true)
	}

	fn spawn_inner<G, F, Fut>(
		&self,
		key: K,
		context: G,
		f: F,
		get: bool,
		detached: bool,
	) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let map = self.0.clone();
		let spawn = {
			let key = key.clone();
			let weak = Arc::downgrade(&map);
			move || {
				let context = context();
				let mut task = Shared::spawn_with_context(context.clone(), {
					let key = key.clone();
					let weak = weak.clone();
					move |stop| {
						let future = f(context, stop);
						async move {
							let result = AssertUnwindSafe(future).catch_unwind().await;
							if let Some(map) = weak.upgrade() {
								let id = tokio::task::id();
								map.remove_if(&key, |_, task| task.id() == id);
							}
							match result {
								Ok(value) => value,
								Err(payload) => std::panic::resume_unwind(payload),
							}
						}
					}
				});
				task.set_on_drop({
					let id = task.id();
					let key = key.clone();
					move || {
						if let Some(map) = weak.upgrade() {
							map.remove_if(&key, |_, task| task.id() == id);
						}
					}
				});
				if detached {
					task.detach();
				}
				task
			}
		};
		let mut task = if get {
			self.0.entry(key).or_insert_with(spawn).value().clone()
		} else {
			let task = spawn();
			self.0.insert(key, task.clone());
			task
		};
		task.attach();
		task
	}

	pub fn spawn_blocking_with_context<G, F>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> T + Send + 'static,
	{
		self.spawn_blocking_inner(key, context, f, false, false)
	}

	pub fn spawn_blocking_detached_with_context<G, F>(
		&self,
		key: K,
		context: G,
		f: F,
	) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> T + Send + 'static,
	{
		self.spawn_blocking_inner(key, context, f, false, true)
	}

	pub fn get_or_spawn_blocking_with_context<G, F>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> T + Send + 'static,
	{
		self.spawn_blocking_inner(key, context, f, true, false)
	}

	pub fn get_or_spawn_blocking_detached_with_context<G, F>(
		&self,
		key: K,
		context: G,
		f: F,
	) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> T + Send + 'static,
	{
		self.spawn_blocking_inner(key, context, f, true, true)
	}

	fn spawn_blocking_inner<G, F>(
		&self,
		key: K,
		context: G,
		f: F,
		get: bool,
		detached: bool,
	) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stop) -> T + Send + 'static,
	{
		let map = self.0.clone();
		let spawn = {
			let key = key.clone();
			let weak = Arc::downgrade(&map);
			move || {
				let context = context();
				let mut task = Shared::spawn_blocking_with_context(context.clone(), {
					let key = key.clone();
					let weak = weak.clone();
					move |stop| {
						let result = catch_unwind(AssertUnwindSafe(|| f(context, stop)));
						if let Some(map) = weak.upgrade() {
							let id = tokio::task::id();
							map.remove_if(&key, |_, task| task.id() == id);
						}
						match result {
							Ok(value) => value,
							Err(payload) => std::panic::resume_unwind(payload),
						}
					}
				});
				task.set_on_drop({
					let id = task.id();
					let key = key.clone();
					move || {
						if let Some(map) = weak.upgrade() {
							map.remove_if(&key, |_, task| task.id() == id);
						}
					}
				});
				if detached {
					task.detach();
				}
				task
			}
		};
		let mut task = if get {
			self.0.entry(key).or_insert_with(spawn).value().clone()
		} else {
			let task = spawn();
			self.0.insert(key, task.clone());
			task
		};
		task.attach();
		task
	}

	pub fn try_get_id(&self, key: &K) -> Option<tokio::task::Id> {
		self.0.get(key).map(|task| task.id())
	}

	pub fn try_get(&self, key: &K) -> Option<Shared<T, C>> {
		self.0.get(key).map(|entry| {
			let mut task = entry.value().clone();
			task.attach();
			task
		})
	}

	pub fn abort(&self, key: &K) {
		if let Some(task) = self.0.get(key) {
			task.abort();
		}
	}

	pub fn stop(&self, key: &K) {
		if let Some(task) = self.0.get(key) {
			task.stop();
		}
	}

	pub fn abort_all(&self) {
		for task in &*self.0 {
			task.abort();
		}
	}

	pub fn stop_all(&self) {
		for task in &*self.0 {
			task.stop();
		}
	}

	pub async fn wait(&self) -> Vec<Result<T, Arc<tokio::task::JoinError>>> {
		let tasks = self
			.0
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

	pub fn spawn_detached<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_detached_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn get_or_spawn<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.get_or_spawn_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn get_or_spawn_detached<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.get_or_spawn_detached_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn spawn_blocking<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		self.spawn_blocking_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn spawn_blocking_detached<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		self.spawn_blocking_detached_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn get_or_spawn_blocking<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		self.get_or_spawn_blocking_with_context(key, || (), |(), stop| f(stop))
	}

	pub fn get_or_spawn_blocking_detached<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		self.get_or_spawn_blocking_detached_with_context(key, || (), |(), stop| f(stop))
	}
}

impl<K, T, C, H> Clone for Map<K, T, C, H> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
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
		Self(Arc::new(DashMap::default()))
	}
}
