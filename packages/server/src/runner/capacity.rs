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
	overcommit: tg::runner::Overcommit,
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
	#[cfg(target_os = "linux")]
	cpu_pool: Mutex<Option<tangram_sandbox::cpu::Pool>>,
	next_index: AtomicU64,
	overcommit: tg::runner::Overcommit,
	threads_per_core: u64,
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
	pub fn new(
		total: tg::runner::Capacity,
		overcommit: tg::runner::Overcommit,
		threads_per_core: u64,
	) -> Self {
		Self {
			state: Arc::new(State {
				allocations: Mutex::new(BTreeMap::new()),
				changed: tokio::sync::Notify::new(),
				#[cfg(target_os = "linux")]
				cpu_pool: Mutex::new(None),
				next_index: AtomicU64::new(0),
				overcommit,
				threads_per_core,
				total,
			}),
		}
	}

	#[cfg(target_os = "linux")]
	pub fn set_cpu_pool(&self, pool: Option<tangram_sandbox::cpu::Pool>) {
		*self.state.cpu_pool.lock().unwrap() = pool;
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
		let overcommit = self.state.overcommit;
		let cpu_threads_per_core = self.state.threads_per_core;
		let available = allocations.values().fold(total, |available, allocation| {
			available.subtract(*allocation, overcommit)
		});
		#[cfg(target_os = "linux")]
		let available = if let Some(pool) = &*self.state.cpu_pool.lock().unwrap() {
			let cpu = pool.available();
			let cpu = tg::sandbox::Cpu::new(
				cpu.min.min(available.cpu.min),
				cpu.max.min(available.cpu.max),
			);
			tg::runner::Capacity { cpu, ..available }
		} else {
			available
		};

		let cpu_limit = total.cpu.max.millis() / (1000 * overcommit.cpu);
		tg::runner::control::Capacity {
			available,
			cpu_limit,
			cpu_threads_per_core,
			overcommit,
			total,
		}
	}

	#[must_use]
	pub fn try_acquire(&self, capacity: tg::runner::Capacity) -> Option<Allocation> {
		let mut allocations = self.state.allocations.lock().unwrap();
		let current = self.capacity(&allocations);
		if !current.available.contains(capacity, current.overcommit)
			|| capacity.cpu.max.threads() > current.cpu_limit
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
			overcommit: self.state.overcommit,
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
		capacity.try_borrow(requested, parent_allocation.overcommit)?;
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
		if allocation
			.capacity
			.try_borrow(requested, allocation.overcommit)
			.is_none()
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
	pub fn capacity(&self) -> tg::runner::Capacity {
		self.capacity
	}

	#[must_use]
	pub fn try_borrow(
		parent: tokio::sync::OwnedMutexGuard<Option<Self>>,
		requested: tg::runner::Capacity,
	) -> Option<Self> {
		let allocation = parent.as_ref()?;
		let capacity = allocation
			.capacity
			.try_borrow(requested, allocation.overcommit)?;
		#[cfg(target_os = "linux")]
		let cpu_parent = allocation.cpu_lease.clone();
		let overcommit = allocation.overcommit;
		Some(Self {
			capacity,
			#[cfg(target_os = "linux")]
			cpu_lease: None,
			#[cfg(target_os = "linux")]
			cpu_parent,
			overcommit,
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

	fn capacity(min: u64, max: u64) -> tg::runner::Capacity {
		let cpu = tg::sandbox::Cpu::new(
			tg::sandbox::Amount::from_millis(min),
			tg::sandbox::Amount::from_millis(max),
		);
		tg::runner::Capacity {
			cpu,
			memory: tg::sandbox::Resource::new(min, max),
			storage: tg::sandbox::Resource::new(min, max),
			swap: tg::sandbox::Resource::new(min, max),
		}
	}

	#[test]
	fn admission_reserves_all_minima_and_recovers_capacity_on_release() {
		let total = capacity(2000, 8000);
		let overcommit = tg::runner::Overcommit {
			cpu: 4,
			memory: 4,
			storage: 4,
			swap: 4,
		};
		let pool = Pool::new(total, overcommit, 2);
		let first = pool.try_acquire(capacity(250, 1000)).unwrap();
		assert_eq!(pool.get().cpu_limit, 2);
		assert_eq!(
			pool.get().available.memory,
			tg::sandbox::Resource::new(1562, 6250)
		);
		assert!(pool.try_acquire(capacity(1600, 2000)).is_none());
		let second = pool.try_acquire(capacity(0, 1000)).unwrap();
		drop(first);
		drop(second);
		assert_eq!(pool.get().available, total);
	}

	#[test]
	fn a_borrower_cannot_promote_a_maximum_into_a_new_minimum() {
		let total = capacity(2000, 8000);
		let pool = Pool::new(total, tg::runner::Overcommit::default(), 2);
		let parent = pool.try_acquire(capacity(0, 1000)).unwrap();
		let parent = Arc::new(tokio::sync::Mutex::new(Some(parent)));
		let available = pool.get().available;
		let guard = parent.clone().try_lock_owned().unwrap();
		assert!(Allocation::try_borrow(guard, capacity(250, 1000)).is_none());
		let guard = parent.clone().try_lock_owned().unwrap();
		let child = Allocation::try_borrow(guard, capacity(0, 1000)).unwrap();
		assert_eq!(pool.get().available, available);
		drop(child);
		drop(parent);
		assert_eq!(pool.get().available, total);
	}
}
