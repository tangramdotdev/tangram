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
	#[cfg(target_os = "linux")]
	cpu_lease: Option<tangram_sandbox::cpu::Lease>,
	#[cfg(target_os = "linux")]
	cpu_parent: Option<tangram_sandbox::cpu::Lease>,
	oversubscription: u64,
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
			#[cfg(target_os = "linux")]
			cpu_lease: None,
			#[cfg(target_os = "linux")]
			cpu_parent: None,
			oversubscription: self.state.oversubscription,
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
		let capacity = parent_allocation.capacity;
		if !capacity.contains(requested, parent_allocation.oversubscription) {
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
		if !allocation
			.capacity
			.contains(requested, allocation.oversubscription)
		{
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
		if !allocation
			.capacity
			.contains(requested, allocation.oversubscription)
		{
			return None;
		}
		let capacity = requested;
		#[cfg(target_os = "linux")]
		let cpu_parent = allocation.cpu_lease.clone();
		let oversubscription = allocation.oversubscription;
		Some(Self {
			capacity,
			#[cfg(target_os = "linux")]
			cpu_lease: None,
			#[cfg(target_os = "linux")]
			cpu_parent,
			oversubscription,
			source: AllocationSource::Parent(parent),
		})
	}

	#[cfg(target_os = "linux")]
	#[must_use]
	pub fn cpu_parent(&self) -> Option<tangram_sandbox::cpu::Lease> {
		self.cpu_parent.clone()
	}

	#[cfg(target_os = "linux")]
	pub fn set_cpu_lease(&mut self, lease: Option<tangram_sandbox::cpu::Lease>) {
		self.cpu_lease = lease;
		self.cpu_parent.take();
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

#[cfg(test)]
mod tests {
	use super::*;

	fn capacity(dedicated: u64, shared: u64) -> tg::runner::Capacity {
		let cpu = tg::sandbox::Cpu { dedicated, shared };
		tg::runner::Capacity { cpu, memory: 1 }
	}

	#[test]
	fn dedicated_capacity_can_be_borrowed_as_shared_without_extra_admission() {
		let pool = Pool::new(capacity(1, 0), 4);
		let parent = pool.try_acquire(capacity(1, 0)).unwrap();
		let parent = Arc::new(tokio::sync::Mutex::new(Some(parent)));
		let before = pool.get().available;
		let guard = parent.clone().try_lock_owned().unwrap();
		let child = Allocation::try_borrow(guard, capacity(0, 4)).unwrap();
		assert_eq!(child.capacity, capacity(0, 4));
		assert_eq!(pool.get().available, before);
		let child = Arc::new(tokio::sync::Mutex::new(Some(child)));
		let guard = child.clone().try_lock_owned().unwrap();
		assert!(Allocation::try_borrow(guard, capacity(1, 0)).is_none());
		let guard = child.clone().try_lock_owned().unwrap();
		let grandchild = Allocation::try_borrow(guard, capacity(0, 4)).unwrap();
		drop(grandchild);
		drop(child);
		assert!(parent.clone().try_lock_owned().is_ok());
		drop(parent);
		assert_eq!(pool.get().available, capacity(1, 0));
	}

	#[test]
	fn reservations_apply_the_same_conversion_rules_as_shortcuts() {
		let pool = Pool::new(capacity(1, 0), 4);
		let parent = pool.try_acquire(capacity(1, 0)).unwrap();
		let parent = Arc::new(tokio::sync::Mutex::new(Some(parent)));
		let reservations = Reservations::new();
		let id = tg::sandbox::Id::new();
		let guard = parent.clone().try_lock_owned().unwrap();
		assert!(
			reservations
				.reserve(guard, id.clone(), capacity(0, 5))
				.is_none()
		);
		let guard = parent.clone().try_lock_owned().unwrap();
		let (advertised, reservation) = reservations
			.reserve(guard, id.clone(), capacity(0, 4))
			.unwrap();
		assert_eq!(advertised, capacity(1, 0));
		let child = reservations.try_acquire(&id, capacity(0, 4)).unwrap();
		assert_eq!(child.capacity, capacity(0, 4));
		drop(child);
		drop(reservation);
		let guard = parent.clone().try_lock_owned().unwrap();
		assert!(Allocation::try_borrow(guard, capacity(1, 0)).is_some());
	}

	#[test]
	fn shared_capacity_cannot_be_promoted_or_oversubscribed_again() {
		let pool = Pool::new(capacity(1, 0), 4);
		let parent = pool.try_acquire(capacity(0, 1)).unwrap();
		let parent = Arc::new(tokio::sync::Mutex::new(Some(parent)));
		let guard = parent.clone().try_lock_owned().unwrap();
		assert!(Allocation::try_borrow(guard, capacity(1, 0)).is_none());
		let guard = parent.clone().try_lock_owned().unwrap();
		assert!(Allocation::try_borrow(guard, capacity(0, 2)).is_none());
		let guard = parent.clone().try_lock_owned().unwrap();
		assert!(Allocation::try_borrow(guard, capacity(0, 1)).is_some());
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
