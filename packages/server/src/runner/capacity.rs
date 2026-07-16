use {
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
};

#[derive(Clone)]
pub struct Pool {
	state: Arc<State>,
}

pub struct Allocation {
	capacity: tg::runner::Capacity,
	source: AllocationSource,
}

struct State {
	available: Mutex<tg::runner::Capacity>,
	changed: tokio::sync::Notify,
	total: tg::runner::Capacity,
}

enum AllocationSource {
	Parent {
		_guard: tokio::sync::OwnedMutexGuard<Option<Allocation>>,
	},
	Pool(Pool),
}

impl Pool {
	pub fn new(total: tg::runner::Capacity) -> Self {
		Self {
			state: Arc::new(State {
				available: Mutex::new(total),
				changed: tokio::sync::Notify::new(),
				total,
			}),
		}
	}

	pub fn get(&self) -> tg::runner::control::Capacity {
		let available = *self.state.available.lock().unwrap();
		tg::runner::control::Capacity {
			available,
			total: self.state.total,
		}
	}

	pub fn try_acquire(&self, capacity: tg::runner::Capacity) -> Option<Allocation> {
		let mut available = self.state.available.lock().unwrap();
		if !contains(*available, capacity) {
			return None;
		}
		available.cpus -= capacity.cpus;
		available.memory -= capacity.memory;
		drop(available);
		self.state.changed.notify_one();
		Some(Allocation {
			capacity,
			source: AllocationSource::Pool(self.clone()),
		})
	}

	pub async fn wait_for_change(&self) {
		self.state.changed.notified().await;
	}

	fn release(&self, capacity: tg::runner::Capacity) {
		let mut available = self.state.available.lock().unwrap();
		available.cpus += capacity.cpus;
		available.memory += capacity.memory;
		drop(available);
		self.state.changed.notify_one();
	}
}

impl Allocation {
	pub fn try_borrow(
		parent: tokio::sync::OwnedMutexGuard<Option<Self>>,
		requested: tg::runner::Capacity,
	) -> Option<Self> {
		let allocation = parent.as_ref()?;
		if !contains(allocation.capacity, requested) {
			return None;
		}
		let capacity = allocation.capacity;
		Some(Self {
			capacity,
			source: AllocationSource::Parent { _guard: parent },
		})
	}

	pub fn capacity(&self) -> tg::runner::Capacity {
		self.capacity
	}
}

impl Drop for Allocation {
	fn drop(&mut self) {
		if let AllocationSource::Pool(pool) = &self.source {
			pool.release(self.capacity);
		}
	}
}

fn contains(capacity: tg::runner::Capacity, requested: tg::runner::Capacity) -> bool {
	capacity.cpus >= requested.cpus && capacity.memory >= requested.memory
}
