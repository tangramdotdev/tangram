use {
	std::{
		collections::HashMap,
		sync::{
			Arc, Mutex,
			atomic::{AtomicU64, Ordering},
		},
	},
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

#[derive(Clone)]
pub struct Reservations {
	state: Arc<ReservationsState>,
}

pub struct ReservationGuard {
	consumed: tokio::sync::oneshot::Receiver<()>,
	index: u64,
	parent: tg::sandbox::Id,
	reservations: Reservations,
}

struct State {
	available: Mutex<tg::runner::Capacity>,
	changed: tokio::sync::Notify,
	total: tg::runner::Capacity,
}

struct ReservationsState {
	entries: Mutex<HashMap<tg::sandbox::Id, Reservation, tg::id::BuildHasher>>,
	next_index: AtomicU64,
}

struct Reservation {
	allocation: tokio::sync::OwnedMutexGuard<Option<Allocation>>,
	consumed: tokio::sync::oneshot::Sender<()>,
	index: u64,
}

enum AllocationSource {
	Parent(#[allow(dead_code)] tokio::sync::OwnedMutexGuard<Option<Allocation>>),
	Pool(Pool),
}

impl Pool {
	#[must_use]
	pub fn new(total: tg::runner::Capacity) -> Self {
		Self {
			state: Arc::new(State {
				available: Mutex::new(total),
				changed: tokio::sync::Notify::new(),
				total,
			}),
		}
	}

	#[must_use]
	pub fn get(&self) -> tg::runner::control::Capacity {
		let available = *self.state.available.lock().unwrap();
		tg::runner::control::Capacity {
			available,
			total: self.state.total,
		}
	}

	#[must_use]
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

impl Reservations {
	#[must_use]
	pub fn new() -> Self {
		let state = ReservationsState {
			entries: Mutex::new(HashMap::default()),
			next_index: AtomicU64::new(0),
		};
		Self {
			state: Arc::new(state),
		}
	}

	#[must_use]
	pub fn reserve(
		&self,
		allocation: tokio::sync::OwnedMutexGuard<Option<Allocation>>,
		parent: tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> Option<(tg::runner::Capacity, ReservationGuard)> {
		let capacity = allocation.as_ref()?.capacity;
		if !contains(capacity, requested) {
			return None;
		}
		let index = self.state.next_index.fetch_add(1, Ordering::Relaxed);
		let (consumed, receiver) = tokio::sync::oneshot::channel();
		let reservation = Reservation {
			allocation,
			consumed,
			index,
		};
		let previous = self
			.state
			.entries
			.lock()
			.unwrap()
			.insert(parent.clone(), reservation);
		debug_assert!(previous.is_none());
		let guard = ReservationGuard {
			consumed: receiver,
			index,
			parent,
			reservations: self.clone(),
		};

		Some((capacity, guard))
	}

	#[must_use]
	pub fn try_acquire(
		&self,
		parent: &tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> Option<Allocation> {
		let mut entries = self.state.entries.lock().unwrap();
		let reservation = entries.remove(parent)?;
		let allocation = reservation.allocation.as_ref()?;
		if !contains(allocation.capacity, requested) {
			entries.insert(parent.clone(), reservation);
			return None;
		}
		drop(entries);
		if reservation.consumed.send(()).is_err() {
			return None;
		}

		Allocation::try_borrow(reservation.allocation, requested)
	}
}

impl Allocation {
	#[must_use]
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
			source: AllocationSource::Parent(parent),
		})
	}
}

impl ReservationGuard {
	pub async fn wait(&mut self) {
		(&mut self.consumed).await.ok();
	}
}

impl Drop for ReservationGuard {
	fn drop(&mut self) {
		let mut entries = self.reservations.state.entries.lock().unwrap();
		let remove = entries
			.get(&self.parent)
			.is_some_and(|reservation| reservation.index == self.index);
		if remove {
			entries.remove(&self.parent);
		}
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

#[cfg(test)]
mod tests {
	use {super::*, std::sync::Arc};

	#[tokio::test]
	async fn a_stale_guard_does_not_release_a_new_reservation() {
		let capacity = tg::runner::Capacity { cpus: 1, memory: 1 };
		let pool = Pool::new(capacity);
		let allocation = pool.try_acquire(capacity).unwrap();
		let allocation = Arc::new(tokio::sync::Mutex::new(Some(allocation)));
		let parent = tg::sandbox::Id::new();
		let reservations = Reservations::new();

		let parent_allocation = allocation.clone().lock_owned().await;
		let (_, mut first) = reservations
			.reserve(parent_allocation, parent.clone(), capacity)
			.unwrap();
		let child = reservations.try_acquire(&parent, capacity).unwrap();
		first.wait().await;
		drop(child);

		let parent_allocation = allocation.clone().lock_owned().await;
		let (_, mut second) = reservations
			.reserve(parent_allocation, parent.clone(), capacity)
			.unwrap();
		drop(first);
		let child = reservations.try_acquire(&parent, capacity).unwrap();
		second.wait().await;
		drop(child);
	}

	#[tokio::test]
	async fn dropping_a_guard_releases_an_unconsumed_reservation() {
		let capacity = tg::runner::Capacity { cpus: 1, memory: 1 };
		let pool = Pool::new(capacity);
		let parent_allocation = pool.try_acquire(capacity).unwrap();
		let parent_allocation = Arc::new(tokio::sync::Mutex::new(Some(parent_allocation)));
		let parent = tg::sandbox::Id::new();
		let reservations = Reservations::new();

		let allocation = parent_allocation.clone().lock_owned().await;
		let (_, reservation) = reservations
			.reserve(allocation, parent.clone(), capacity)
			.unwrap();
		drop(reservation);

		assert!(reservations.try_acquire(&parent, capacity).is_none());
		assert!(parent_allocation.try_lock_owned().is_ok());
	}
}
