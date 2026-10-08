use {
	std::{
		collections::{BTreeMap, HashMap},
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
	allocations: Mutex<BTreeMap<u64, tg::runner::Capacity>>,
	changed: tokio::sync::Notify,
	next_index: AtomicU64,
	oversubscription: u64,
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
	Pool { index: u64, pool: Pool },
}

impl Pool {
	#[must_use]
	pub fn new(total: tg::runner::Capacity, oversubscription: u64) -> Self {
		Self {
			state: Arc::new(State {
				allocations: Mutex::new(BTreeMap::new()),
				changed: tokio::sync::Notify::new(),
				next_index: AtomicU64::new(0),
				oversubscription,
				total,
			}),
		}
	}

	#[must_use]
	pub fn get(&self) -> tg::runner::control::Capacity {
		let allocations = self.state.allocations.lock().unwrap();
		self.capacity(&allocations)
	}

	fn capacity(
		&self,
		allocations: &BTreeMap<u64, tg::runner::Capacity>,
	) -> tg::runner::control::Capacity {
		let total = self.state.total;
		let cpu_oversubscription = self.state.oversubscription;
		let mut used = tg::runner::Capacity::default();
		let mut width = 0;
		for allocation in allocations.values() {
			used.cpu.dedicated += allocation.cpu.dedicated;
			used.cpu.shared += allocation.cpu.shared;
			used.memory += allocation.memory;
			width = width.max(allocation.cpu.shared);
		}
		let (dedicated, shared, shared_cpu_limit) = if total.cpu.dedicated == 0 {
			(
				0,
				total.cpu.shared.saturating_sub(used.cpu.shared),
				total.cpu.shared / cpu_oversubscription,
			)
		} else {
			let shared_cpu_limit = total.cpu.dedicated.saturating_sub(used.cpu.dedicated);
			let occupied = used.cpu.shared.div_ceil(cpu_oversubscription).max(width);
			let dedicated = shared_cpu_limit.saturating_sub(occupied);
			let shared = occupied
				.saturating_mul(cpu_oversubscription)
				.saturating_sub(used.cpu.shared);
			(dedicated, shared, shared_cpu_limit)
		};
		let cpu = tg::sandbox::Cpu { dedicated, shared };
		let memory = total.memory.saturating_sub(used.memory);
		let available = tg::runner::Capacity { cpu, memory };
		tg::runner::control::Capacity {
			available,
			cpu_oversubscription,
			shared_cpu_limit,
			total,
		}
	}

	#[must_use]
	pub fn try_acquire(&self, capacity: tg::runner::Capacity) -> Option<Allocation> {
		let mut allocations = self.state.allocations.lock().unwrap();
		let current = self.capacity(&allocations);
		if !current
			.available
			.contains(capacity, current.cpu_oversubscription)
			|| capacity.cpu.shared
				> current
					.shared_cpu_limit
					.saturating_sub(capacity.cpu.dedicated)
		{
			return None;
		}
		let index = self.state.next_index.fetch_add(1, Ordering::Relaxed);
		allocations.insert(index, capacity);
		drop(allocations);
		self.state.changed.notify_one();
		let pool = self.clone();
		Some(Allocation {
			capacity,
			source: AllocationSource::Pool { index, pool },
		})
	}

	pub async fn wait_for_change(&self) {
		self.state.changed.notified().await;
	}

	fn release(&self, index: u64) {
		self.state.allocations.lock().unwrap().remove(&index);
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
		let parent_allocation = allocation.as_ref()?;
		// A borrowed child would create a second cgroup assignment for the same pool slots.
		if parent_allocation.exclusive_pool() {
			return None;
		}
		let capacity = parent_allocation.capacity;
		if capacity.cpu.dedicated != 0 || requested.cpu.dedicated != 0 {
			return None;
		}
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
		if allocation.exclusive_pool() {
			return None;
		}
		if allocation.capacity.cpu.dedicated != 0 || requested.cpu.dedicated != 0 {
			return None;
		}
		if !contains(allocation.capacity, requested) {
			return None;
		}
		let capacity = allocation.capacity;
		Some(Self {
			capacity,
			source: AllocationSource::Parent(parent),
		})
	}
	fn exclusive_pool(&self) -> bool {
		match &self.source {
			AllocationSource::Parent(parent) => parent.as_ref().is_some_and(Self::exclusive_pool),
			AllocationSource::Pool { pool, .. } => pool.state.total.cpu.dedicated != 0,
		}
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
		if let AllocationSource::Pool { index, pool } = &self.source {
			pool.release(*index);
		}
	}
}

fn contains(capacity: tg::runner::Capacity, requested: tg::runner::Capacity) -> bool {
	capacity.cpu.shared >= requested.cpu.shared
		&& capacity.cpu.dedicated >= requested.cpu.dedicated
		&& capacity.memory >= requested.memory
}

#[cfg(test)]
mod tests {
	use super::*;

	fn capacity(dedicated: u64, shared: u64) -> tg::runner::Capacity {
		let cpu = tg::sandbox::Cpu { dedicated, shared };
		tg::runner::Capacity { cpu, memory: 1 }
	}

	#[test]
	fn converts_free_cores_to_shared_slots_and_reclaims_them() {
		let mut total = capacity(3, 0);
		total.memory = 32;
		let pool = Pool::new(total, 4);
		let first = pool.try_acquire(capacity(0, 1)).unwrap();
		assert_eq!(
			pool.get().available.cpu,
			tg::sandbox::Cpu {
				dedicated: 2,
				shared: 3
			}
		);
		let second = pool.try_acquire(capacity(0, 1)).unwrap();
		assert_eq!(
			pool.get().available.cpu,
			tg::sandbox::Cpu {
				dedicated: 2,
				shared: 2
			}
		);
		let dedicated = pool.try_acquire(capacity(2, 0)).unwrap();
		assert!(pool.try_acquire(capacity(1, 0)).is_none());
		drop(first);
		drop(second);
		assert_eq!(
			pool.get().available.cpu,
			tg::sandbox::Cpu {
				dedicated: 1,
				shared: 0
			}
		);
		drop(dedicated);
		assert_eq!(pool.get().available, total);
	}

	#[test]
	fn accounts_for_distinct_shared_cores_and_mixed_requests() {
		let mut total = capacity(3, 0);
		total.memory = 32;
		let pool = Pool::new(total, 4);
		let shared = pool.try_acquire(capacity(0, 2)).unwrap();
		assert_eq!(
			pool.get().available.cpu,
			tg::sandbox::Cpu {
				dedicated: 1,
				shared: 6
			}
		);
		assert!(pool.try_acquire(capacity(2, 0)).is_none());
		assert!(pool.try_acquire(capacity(1, 3)).is_none());
		let mixed = pool.try_acquire(capacity(1, 2)).unwrap();
		assert_eq!(
			pool.get().available.cpu,
			tg::sandbox::Cpu {
				dedicated: 0,
				shared: 4
			}
		);
		drop(mixed);
		drop(shared);
		assert_eq!(pool.get().available, total);
	}
}
