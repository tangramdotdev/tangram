use {
	crate::task::{Shared, Stopper},
	dashmap::DashMap,
	std::{
		collections::hash_map::RandomState,
		future::Future,
		hash::{BuildHasher, Hash},
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
		F: FnOnce(C, Stopper) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_inner(key, context, f, false, false)
	}

	pub fn spawn_detached_with_context<G, F, Fut>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stopper) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_inner(key, context, f, false, true)
	}

	pub fn get_or_spawn_with_context<G, F, Fut>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stopper) -> Fut,
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
		F: FnOnce(C, Stopper) -> Fut,
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
		F: FnOnce(C, Stopper) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let mut task = if get {
			match self.0.entry(key.clone()) {
				dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
				dashmap::mapref::entry::Entry::Vacant(entry) => {
					let context = context();
					let mut task = Shared::spawn_with_context(context.clone(), move |stopper| {
						f(context, stopper)
					});
					if detached {
						task.detach();
					}
					entry.insert(task.clone());
					self.spawn_waiter(key, &task);
					task
				},
			}
		} else {
			let context = context();
			let mut task =
				Shared::spawn_with_context(context.clone(), move |stopper| f(context, stopper));
			if detached {
				task.detach();
			}
			self.0.insert(key.clone(), task.clone());
			self.spawn_waiter(key, &task);
			task
		};
		task.attach();
		task
	}

	pub fn spawn_blocking_with_context<G, F>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stopper) -> T + Send + 'static,
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
		F: FnOnce(C, Stopper) -> T + Send + 'static,
	{
		self.spawn_blocking_inner(key, context, f, false, true)
	}

	pub fn get_or_spawn_blocking_with_context<G, F>(&self, key: K, context: G, f: F) -> Shared<T, C>
	where
		G: FnOnce() -> C,
		F: FnOnce(C, Stopper) -> T + Send + 'static,
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
		F: FnOnce(C, Stopper) -> T + Send + 'static,
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
		F: FnOnce(C, Stopper) -> T + Send + 'static,
	{
		let mut task = if get {
			match self.0.entry(key.clone()) {
				dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
				dashmap::mapref::entry::Entry::Vacant(entry) => {
					let context = context();
					let mut task =
						Shared::spawn_blocking_with_context(context.clone(), move |stopper| {
							f(context, stopper)
						});
					if detached {
						task.detach();
					}
					entry.insert(task.clone());
					self.spawn_waiter(key, &task);
					task
				},
			}
		} else {
			let context = context();
			let mut task = Shared::spawn_blocking_with_context(context.clone(), move |stopper| {
				f(context, stopper)
			});
			if detached {
				task.detach();
			}
			self.0.insert(key.clone(), task.clone());
			self.spawn_waiter(key, &task);
			task
		};
		task.attach();
		task
	}

	fn spawn_waiter(&self, key: K, task: &Shared<T, C>) {
		let mut waiter = task.clone();
		waiter.detach();
		let task_id = task.id();
		let weak = Arc::downgrade(&self.0);
		tokio::spawn(async move {
			waiter.wait().await.ok();
			if let Some(map) = weak.upgrade() {
				map.remove_if(&key, |_, task| task.id() == task_id);
			}
		});
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

	#[must_use]
	pub fn len(&self) -> usize {
		self.0.len()
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
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
		for entry in &*self.0 {
			entry.value().abort();
		}
	}

	pub fn stop_all(&self) {
		for entry in &*self.0 {
			entry.value().stop();
		}
	}

	pub async fn wait(&self) -> Vec<Result<T, Arc<tokio::task::JoinError>>> {
		let mut results = Vec::new();
		while let Some((key, id, task)) = self.0.iter().next().map(|entry| {
			(
				entry.key().clone(),
				entry.value().id(),
				entry.value().clone(),
			)
		}) {
			let result = task.wait().await;
			self.0.remove_if(&key, |_, task| task.id() == id);
			results.push(result);
		}
		results
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
		F: FnOnce(Stopper) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_with_context(key, || (), |(), stopper| f(stopper))
	}

	pub fn spawn_detached<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stopper) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.spawn_detached_with_context(key, || (), |(), stopper| f(stopper))
	}

	pub fn get_or_spawn<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stopper) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.get_or_spawn_with_context(key, || (), |(), stopper| f(stopper))
	}

	pub fn get_or_spawn_detached<F, Fut>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stopper) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		self.get_or_spawn_detached_with_context(key, || (), |(), stopper| f(stopper))
	}

	pub fn spawn_blocking<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stopper) -> T + Send + 'static,
	{
		self.spawn_blocking_with_context(key, || (), |(), stopper| f(stopper))
	}

	pub fn spawn_blocking_detached<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stopper) -> T + Send + 'static,
	{
		self.spawn_blocking_detached_with_context(key, || (), |(), stopper| f(stopper))
	}

	pub fn get_or_spawn_blocking<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stopper) -> T + Send + 'static,
	{
		self.get_or_spawn_blocking_with_context(key, || (), |(), stopper| f(stopper))
	}

	pub fn get_or_spawn_blocking_detached<F>(&self, key: K, f: F) -> Shared<T, ()>
	where
		F: FnOnce(Stopper) -> T + Send + 'static,
	{
		self.get_or_spawn_blocking_detached_with_context(key, || (), |(), stopper| f(stopper))
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
