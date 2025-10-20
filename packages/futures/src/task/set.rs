use {
	crate::task::{Map, Shared, Stop},
	futures::Future,
	std::{
		hash::BuildHasher,
		sync::{
			atomic::{AtomicU64, Ordering},
			Arc,
		},
	},
};

pub struct Set<T, H = std::collections::hash_map::RandomState> {
	map: Map<u64, T, H>,
	next_id: Arc<AtomicU64>,
}

impl<T, H> Set<T, H>
where
	T: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	pub fn spawn<F, Fut>(&self, f: F) -> Shared<T>
	where
		F: FnOnce(Stop) -> Fut,
		Fut: Future<Output = T> + Send + 'static,
	{
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		self.map.spawn(id, f)
	}

	pub fn spawn_blocking<F>(&self, f: F) -> Shared<T>
	where
		F: FnOnce(Stop) -> T + Send + 'static,
	{
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		self.map.get_or_spawn_blocking(id, f)
	}

	pub fn abort_all(&self) {
		self.map.abort_all();
	}

	pub fn stop_all(&self) {
		self.map.stop_all();
	}

	pub async fn wait(&self) -> Vec<Result<T, Arc<tokio::task::JoinError>>> {
		self.map.wait().await
	}
}

impl<T, H> Default for Set<T, H>
where
	T: Clone + Send + Sync + 'static,
	H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
	fn default() -> Self {
		Self {
			map: Map::default(),
			next_id: Arc::new(AtomicU64::new(0)),
		}
	}
}

impl<T, H> Clone for Set<T, H> {
	fn clone(&self) -> Self {
		Self {
			map: self.map.clone(),
			next_id: self.next_id.clone(),
		}
	}
}
