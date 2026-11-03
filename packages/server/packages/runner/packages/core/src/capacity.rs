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
	cpu_lender: Option<tangram_sandbox::cpu::ShareId>,
	#[cfg(target_os = "linux")]
	cpu_share: Option<tangram_sandbox::cpu::ShareId>,
	#[allow(dead_code)]
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
	overcommit: tg::runner::Overcommit,
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

/// The source of an allocation's capacity, which holds the capacity until it is dropped.
#[allow(dead_code)]
enum AllocationSource {
	Parent {
		acquisition: Option<Acquisition>,
		parent: tokio::sync::OwnedMutexGuard<Option<Allocation>>,
	},
	Pool(Acquisition),
}

/// Capacity acquired from the pool, which is released when it is dropped.
struct Acquisition {
	index: u64,
	pool: Pool,
}

impl Pool {
	#[must_use]
	pub fn new(total: tg::runner::Capacity, overcommit: tg::runner::Overcommit) -> Self {
		Self {
			state: Arc::new(State {
				allocations: Mutex::new(BTreeMap::new()),
				changed: tokio::sync::Notify::new(),
				next_index: AtomicU64::new(0),
				overcommit,
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
		let overcommit = self.state.overcommit;
		let available = allocations.values().fold(total, |available, allocation| {
			available.subtract(*allocation, overcommit)
		});
		let cpu_limit = total
			.cpu
			.max
			.map_or(0, |max| max.millis() / (1000 * overcommit.cpu));
		tg::runner::control::Capacity {
			available,
			cpu_limit,
			overcommit,
			total,
		}
	}

	#[must_use]
	pub fn try_acquire(&self, capacity: tg::runner::Capacity) -> Option<Allocation> {
		let acquisition = self.try_acquire_inner(capacity)?;
		Some(Allocation {
			capacity,
			#[cfg(target_os = "linux")]
			cpu_lender: None,
			#[cfg(target_os = "linux")]
			cpu_share: None,
			source: AllocationSource::Pool(acquisition),
		})
	}

	fn try_acquire_inner(&self, capacity: tg::runner::Capacity) -> Option<Acquisition> {
		let mut allocations = self.state.allocations.lock().unwrap();
		let current = self.capacity(&allocations);
		if !current.available.contains(capacity, current.overcommit)
			|| capacity
				.cpu
				.max
				.is_some_and(|max| max.threads() > current.cpu_limit)
		{
			return None;
		}
		let index = self.state.next_index.fetch_add(1, Ordering::Relaxed);
		allocations.insert(index, capacity);
		drop(allocations);
		self.state.changed.notify_one();
		let pool = self.clone();
		Some(Acquisition { index, pool })
	}

	pub async fn wait_for_change(&self) {
		self.state.changed.notified().await;
	}

	fn release(&self, index: u64) {
		self.state.allocations.lock().unwrap().remove(&index);
		self.state.changed.notify_one();
	}

	/// Replace an acquisition's capacity with a smaller one in place, so that the rest is available at once and the kept capacity is never available to another sandbox in between.
	fn shrink(&self, index: u64, capacity: tg::runner::Capacity) {
		self.state
			.allocations
			.lock()
			.unwrap()
			.insert(index, capacity);
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
		capacity.try_borrow(requested)?;
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
		pool: &Pool,
		parent: &tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> Option<Allocation> {
		let mut entries = self.state.entries.lock().unwrap();
		let reservation = entries.remove(parent)?;
		let allocation = reservation.allocation.as_ref()?;
		let current = pool.get();
		let borrowable = allocation
			.capacity
			.try_borrow(requested)
			.is_some_and(|(_, acquired)| current.available.contains(acquired, current.overcommit));
		if !borrowable {
			entries.insert(parent.clone(), reservation);
			return None;
		}
		drop(entries);
		if reservation.consumed.send(()).is_err() {
			return None;
		}

		Allocation::try_borrow(pool, reservation.allocation, requested)
	}
}

impl Allocation {
	#[must_use]
	pub fn capacity(&self) -> tg::runner::Capacity {
		self.capacity
	}

	/// Keep only this allocation's storage, releasing the rest, for a stopped sandbox whose directory still holds its data. An allocation borrowed from a parent has no capacity of its own to keep, so this acquires the storage from the pool instead, which returns `None` if it does not fit.
	#[must_use]
	pub fn into_storage(self, pool: &Pool) -> Option<Self> {
		let capacity = tg::runner::Capacity {
			storage: self.capacity.storage,
			..Default::default()
		};
		match self.source {
			AllocationSource::Pool(acquisition) => {
				acquisition.pool.shrink(acquisition.index, capacity);
				let allocation = Self {
					capacity,
					#[cfg(target_os = "linux")]
					cpu_lender: None,
					#[cfg(target_os = "linux")]
					cpu_share: None,
					source: AllocationSource::Pool(acquisition),
				};
				Some(allocation)
			},
			source @ AllocationSource::Parent { .. } => {
				// Release the borrowed capacity and the parent before acquiring the storage, so that the storage fits whenever it would once they are released.
				drop(source);
				pool.try_acquire(capacity)
			},
		}
	}

	/// Borrow the requested capacity from a waiting parent's allocation, acquiring from the pool the minima that the parent keeps.
	#[must_use]
	pub fn try_borrow(
		pool: &Pool,
		parent: tokio::sync::OwnedMutexGuard<Option<Self>>,
		requested: tg::runner::Capacity,
	) -> Option<Self> {
		let allocation = parent.as_ref()?;
		let (capacity, acquired) = allocation.capacity.try_borrow(requested)?;
		let acquisition = if acquired == tg::runner::Capacity::default() {
			None
		} else {
			Some(pool.try_acquire_inner(acquired)?)
		};
		#[cfg(target_os = "linux")]
		let cpu_lender = allocation.cpu_share;
		Some(Self {
			capacity,
			#[cfg(target_os = "linux")]
			cpu_lender,
			#[cfg(target_os = "linux")]
			cpu_share: None,
			source: AllocationSource::Parent {
				acquisition,
				parent,
			},
		})
	}

	/// Get the CPU share of the sandbox whose CPU minimum this allocation borrows.
	#[cfg(target_os = "linux")]
	#[must_use]
	pub fn cpu_lender(&self) -> Option<tangram_sandbox::cpu::ShareId> {
		self.cpu_lender
	}

	/// Set the CPU share of the sandbox that this allocation belongs to, so that its borrowers can name it as their lender.
	#[cfg(target_os = "linux")]
	pub fn set_cpu_share(&mut self, share: Option<tangram_sandbox::cpu::ShareId>) {
		self.cpu_share = share;
	}
}

impl ReservationGuard {
	pub async fn wait(&mut self) {
		(&mut self.consumed).await.ok();
	}
}

impl Default for Reservations {
	fn default() -> Self {
		Self::new()
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

impl Drop for Acquisition {
	fn drop(&mut self) {
		self.pool.release(self.index);
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
			sandboxes: 1,
			storage: tg::sandbox::Resource::new(min, max),
		}
	}

	#[test]
	fn admission_reserves_all_minima_and_recovers_capacity_on_release() {
		let total = tg::runner::Capacity {
			sandboxes: 4,
			..capacity(2000, 8000)
		};
		let overcommit = tg::runner::Overcommit {
			cpu: 4,
			memory: 4,
			storage: 4,
		};
		let pool = Pool::new(total, overcommit);
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
	fn a_stopped_allocation_keeps_only_its_storage() {
		let total = tg::runner::Capacity {
			sandboxes: 4,
			..capacity(2000, 8000)
		};
		let pool = Pool::new(total, tg::runner::Overcommit::default());
		let allocation = pool.try_acquire(capacity(1000, 2000)).unwrap();

		// Keeping the storage releases the CPU, memory, and sandbox, and keeps the storage unavailable.
		let storage = allocation.into_storage(&pool).unwrap();
		let available = pool.get().available;
		assert_eq!(available.cpu, total.cpu);
		assert_eq!(available.memory, total.memory);
		assert_eq!(available.sandboxes, total.sandboxes);
		assert_eq!(available.storage, tg::sandbox::Resource::new(1000, 6000));
		assert!(pool.try_acquire(capacity(1500, 7000)).is_none());

		// Dropping the storage releases it.
		drop(storage);
		assert_eq!(pool.get().available, total);
	}

	#[test]
	fn a_borrower_takes_its_parent_cpu_minimum_and_acquires_the_rest() {
		let total = tg::runner::Capacity {
			sandboxes: 4,
			..capacity(2000, 8000)
		};
		let pool = Pool::new(total, tg::runner::Overcommit::default());
		let parent = pool.try_acquire(capacity(1000, 2000)).unwrap();
		let parent = Arc::new(tokio::sync::Mutex::new(Some(parent)));
		let available = pool.get().available;

		// A borrower acquires the CPU minimum beyond its parent's from the pool.
		let requested = tg::runner::Capacity {
			cpu: tg::sandbox::Cpu::with_min(tg::sandbox::Amount::from_millis(1500)),
			sandboxes: 1,
			..Default::default()
		};
		let guard = parent.clone().try_lock_owned().unwrap();
		let child = Allocation::try_borrow(&pool, guard, requested).unwrap();
		assert_eq!(
			pool.get().available.cpu.min.millis(),
			available.cpu.min.millis() - 500
		);
		drop(child);

		// A borrower takes its CPU minimum from its parent and acquires its other minima from the pool.
		let requested = tg::runner::Capacity {
			cpu: tg::sandbox::Cpu::with_min(tg::sandbox::Amount::from_millis(500)),
			memory: tg::sandbox::Resource::with_min(500),
			sandboxes: 1,
			..Default::default()
		};
		let guard = parent.clone().try_lock_owned().unwrap();
		let child = Allocation::try_borrow(&pool, guard, requested).unwrap();
		assert_eq!(child.capacity().cpu.min.millis(), 500);
		assert_eq!(
			child.capacity().memory,
			tg::sandbox::Resource::new(500, 1500)
		);
		assert_eq!(pool.get().available.cpu, available.cpu);
		assert_eq!(pool.get().available.memory.min, available.memory.min - 500);

		// Releasing the borrower releases what it acquired.
		drop(child);
		assert_eq!(pool.get().available, available);
		drop(parent);
		assert_eq!(pool.get().available, total);
	}
}
