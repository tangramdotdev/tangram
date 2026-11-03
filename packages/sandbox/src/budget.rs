use {
	std::{
		collections::BTreeMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

#[derive(Clone)]
pub struct Pool(Arc<State>);

pub struct Allocation {
	id: u64,
	pool: Pool,
}

pub(crate) struct Control {
	pub(crate) limit: Box<dyn Fn(u64) -> tg::Result<()> + Send + Sync>,
	pub(crate) usage: Box<dyn Fn() -> tg::Result<u64> + Send + Sync>,
}

struct State {
	capacity: u64,
	entries: Mutex<Bookkeeping>,
	floor: u64,
	unit: u64,
}

#[derive(Default)]
struct Bookkeeping {
	entries: BTreeMap<u64, Entry>,
	next_id: u64,
	poisoned: bool,
}

struct Entry {
	control: Option<Control>,
	limit: u64,
	max: u64,
	min: u64,
	orphaned: bool,
	project: Option<u32>,
}

impl Pool {
	#[must_use]
	pub fn new(capacity: u64, floor: u64, unit: u64) -> Self {
		let state = State {
			capacity,
			entries: Mutex::new(Bookkeeping::default()),
			floor,
			unit: unit.max(1),
		};
		Self(Arc::new(state))
	}

	/// Allocate a resource from the pool for a filesystem project, where a resource without a maximum may grow to the pool's capacity.
	pub(crate) fn allocate(
		&self,
		resource: tg::sandbox::Resource<u64>,
		project: Option<u32>,
	) -> tg::Result<Allocation> {
		resource.validate()?;
		let max = resource.max.unwrap_or(self.0.capacity);
		let min = resource.min;
		if max < self.0.floor || self.round(min)? > max {
			return Err(tg::error!(
				"the resource maximum is smaller than the allocation unit"
			));
		}
		let mut entries = self.0.entries.lock().unwrap();
		if entries.poisoned {
			return Err(tg::error!(
				"the resource pool is unavailable after a failed limit update"
			));
		}

		// Reuse a released entry of the same filesystem project, whose data the pool still counts, so that a sandbox that starts again on its directory does not count the data twice.
		if let Some(project) = project
			&& let Some((&id, entry)) = entries
				.entries
				.iter_mut()
				.find(|(_, entry)| entry.orphaned && entry.project == Some(project))
		{
			let (previous_max, previous_min) = (entry.max, entry.min);
			entry.max = max;
			entry.min = min;
			entry.orphaned = false;
			let limits = limits(&entries);
			if let Err(error) = self.rebalance(&mut entries) {
				if let Some(entry) = entries.entries.get_mut(&id) {
					entry.max = previous_max;
					entry.min = previous_min;
					entry.orphaned = true;
				}
				restore(&mut entries, limits)?;
				return Err(error);
			}
			let allocation = Allocation {
				id,
				pool: self.clone(),
			};
			return Ok(allocation);
		}

		let id = entries.next_id;
		entries.next_id += 1;
		let entry = Entry {
			control: None,
			limit: 0,
			max,
			min,
			orphaned: false,
			project,
		};
		let limits = limits(&entries);
		entries.entries.insert(id, entry);
		if let Err(error) = self.rebalance(&mut entries) {
			entries.entries.remove(&id);
			restore(&mut entries, limits)?;
			return Err(error);
		}
		Ok(Allocation {
			id,
			pool: self.clone(),
		})
	}

	/// Count a stopped sandbox's directory, whose data remains, as a released entry of its filesystem project, so that the pool counts its data and a sandbox that starts again on the directory reuses the entry.
	pub(crate) fn adopt(
		&self,
		resource: tg::sandbox::Resource<u64>,
		project: u32,
		control: Control,
	) -> tg::Result<()> {
		resource.validate()?;
		let max = resource.max.unwrap_or(self.0.capacity);
		let mut entries = self.0.entries.lock().unwrap();
		if entries.poisoned {
			return Err(tg::error!(
				"the resource pool is unavailable after a failed limit update"
			));
		}
		let id = entries.next_id;
		entries.next_id += 1;
		let entry = Entry {
			control: Some(control),
			limit: 0,
			max,
			min: resource.min,
			orphaned: true,
			project: Some(project),
		};
		let limits = limits(&entries);
		entries.entries.insert(id, entry);
		if let Err(error) = self.rebalance(&mut entries) {
			entries.entries.remove(&id);
			restore(&mut entries, limits)?;
			return Err(error);
		}
		Ok(())
	}

	/// Stop counting the released entries of a filesystem project whose data is removed, so that a stale entry does not set quotas on a later sandbox that receives the project.
	pub(crate) fn forget(&self, project: u32) {
		let mut entries = self.0.entries.lock().unwrap();
		entries
			.entries
			.retain(|_, entry| !(entry.orphaned && entry.project == Some(project)));
		if let Err(error) = self.rebalance(&mut entries) {
			entries.poisoned = true;
			tracing::error!(%error, "failed to rebalance the resource pool");
		}
	}

	fn rebalance(&self, entries: &mut Bookkeeping) -> tg::Result<()> {
		// Stop growth before measuring the capacity occupied by persistent data.
		for entry in entries.entries.values_mut() {
			if let Some(control) = &entry.control {
				let usage = (control.usage)()?;
				let floor = self.round(usage.max(entry.min).max(self.0.floor))?;
				if let Err(error) = (control.limit)(floor) {
					entries.poisoned = true;
					return Err(error);
				}
				entry.limit = floor;
			}
		}

		// Retain orphaned allocations until the kernel releases their data.
		let mut floors = BTreeMap::new();
		let mut removable = Vec::new();
		let mut occupied = 0_u64;
		for (id, entry) in &entries.entries {
			let usage = entry
				.control
				.as_ref()
				.map_or(Ok(0), |control| (control.usage)())?;
			if entry.orphaned && usage == 0 {
				removable.push(*id);
				continue;
			}
			let floor = self.round(usage.max(entry.min).max(self.0.floor))?;
			occupied = occupied
				.checked_add(floor)
				.ok_or_else(|| tg::error!("the resource capacity overflowed"))?;
			floors.insert(*id, floor);
		}
		for id in removable {
			entries.entries.remove(&id);
		}
		let remaining = self
			.0
			.capacity
			.checked_sub(occupied)
			.ok_or_else(|| tg::error!("there is not enough backed resource capacity"))?;
		let remaining = remaining / self.0.unit;
		let demand: u128 = entries
			.entries
			.iter()
			.map(|(id, entry)| u128::from(entry.max.saturating_sub(floors[id]) / self.0.unit))
			.sum();
		for (id, entry) in &mut entries.entries {
			let floor = floors[id];
			let need = entry.max.saturating_sub(floor) / self.0.unit;
			let granted = u128::from(remaining) * u128::from(need) / demand.max(1);
			let granted = u64::try_from(granted).unwrap_or(u64::MAX).min(need);
			let limit = floor + granted * self.0.unit;
			if let Some(control) = &entry.control
				&& let Err(error) = (control.limit)(limit)
			{
				entries.poisoned = true;
				return Err(error);
			}
			entry.limit = limit;
		}
		Ok(())
	}

	fn round(&self, bytes: u64) -> tg::Result<u64> {
		bytes
			.div_ceil(self.0.unit)
			.checked_mul(self.0.unit)
			.ok_or_else(|| tg::error!("the resource size overflowed"))
	}
}

impl Allocation {
	/// Release the allocation without its minimum, for a sandbox whose data is being removed, so that the pool counts only the data that remains.
	pub(crate) fn discard(self) {
		let mut entries = self.pool.0.entries.lock().unwrap();
		if let Some(entry) = entries.entries.get_mut(&self.id) {
			entry.min = 0;
		}
		drop(entries);
	}

	#[must_use]
	pub(crate) fn limit(&self) -> u64 {
		self.pool.0.entries.lock().unwrap().entries[&self.id].limit
	}

	pub(crate) fn bind(&self, control: Control) -> tg::Result<()> {
		let mut entries = self.pool.0.entries.lock().unwrap();
		let entry = entries.entries.get_mut(&self.id).unwrap();
		(control.limit)(entry.limit)?;
		entry.control = Some(control);
		Ok(())
	}
}

impl std::fmt::Debug for Pool {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("Pool")
			.field("capacity", &self.0.capacity)
			.finish_non_exhaustive()
	}
}

impl std::fmt::Debug for Allocation {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("Allocation")
			.field("id", &self.id)
			.finish_non_exhaustive()
	}
}

impl Drop for Allocation {
	fn drop(&mut self) {
		let mut entries = self.pool.0.entries.lock().unwrap();
		if let Some(entry) = entries.entries.get_mut(&self.id) {
			entry.orphaned = true;
		}
		if let Err(error) = self.pool.rebalance(&mut entries) {
			entries.poisoned = true;
			tracing::error!(%error, "failed to release the resource allocation");
		}
	}
}

/// Get the current limit of every entry, so that a failed admission can restore them.
fn limits(entries: &Bookkeeping) -> BTreeMap<u64, u64> {
	entries
		.entries
		.iter()
		.map(|(id, entry)| (*id, entry.limit))
		.collect()
}

/// Restore the limits of the entries after a failed admission, because a rebalance clamps every entry before it fails.
fn restore(entries: &mut Bookkeeping, limits: BTreeMap<u64, u64>) -> tg::Result<()> {
	for (id, limit) in limits {
		let Some(entry) = entries.entries.get_mut(&id) else {
			continue;
		};
		if let Some(control) = &entry.control
			&& let Err(error) = (control.limit)(limit)
		{
			entries.poisoned = true;
			return Err(tg::error!(!error, "failed to restore the resource limits"));
		}
		entry.limit = limit;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		std::sync::atomic::{AtomicU64, Ordering},
	};

	fn control(usage: Arc<AtomicU64>, limit: Arc<AtomicU64>) -> Control {
		let usage = Box::new(move || Ok(usage.load(Ordering::SeqCst)));
		let limit = Box::new(move |value| {
			limit.store(value, Ordering::SeqCst);
			Ok(())
		});
		Control { limit, usage }
	}

	#[test]
	fn excess_consumption_cannot_use_another_allocations_unused_minimum() {
		let pool = Pool::new(100, 0, 1);
		let first = pool
			.allocate(tg::sandbox::Resource::new(80, 100), None)
			.unwrap();
		let usage = Arc::new(AtomicU64::new(0));
		let limit = Arc::new(AtomicU64::new(0));
		first.bind(control(usage.clone(), limit.clone())).unwrap();
		let second = pool.allocate(100.into(), None).unwrap();
		assert!(limit.load(Ordering::SeqCst) >= 80);
		assert!(second.limit() <= 20);
		assert!(first.limit() + second.limit() <= 100);
	}

	#[test]
	fn a_released_allocation_is_reused_by_its_project() {
		let pool = Pool::new(100, 0, 1);
		let first = pool
			.allocate(tg::sandbox::Resource::new(60, 60), Some(1))
			.unwrap();
		let usage = Arc::new(AtomicU64::new(10));
		let limit = Arc::new(AtomicU64::new(0));
		first.bind(control(usage, limit)).unwrap();

		// A released allocation keeps its minimum while its data remains, so another project cannot take it.
		drop(first);
		assert!(
			pool.allocate(tg::sandbox::Resource::new(60, 60), Some(2))
				.is_err()
		);

		// The same project reuses the released allocation rather than counting its data twice.
		let second = pool
			.allocate(tg::sandbox::Resource::new(60, 60), Some(1))
			.unwrap();
		assert_eq!(second.limit(), 60);
	}

	#[test]
	fn an_adopted_directory_is_counted_and_reused_by_its_project() {
		let pool = Pool::new(100, 0, 1);
		let usage = Arc::new(AtomicU64::new(10));
		let limit = Arc::new(AtomicU64::new(0));
		pool.adopt(tg::sandbox::Resource::new(60, 60), 1, control(usage, limit))
			.unwrap();

		// The adopted directory keeps its minimum, so another project cannot take it.
		assert!(
			pool.allocate(tg::sandbox::Resource::new(60, 60), Some(2))
				.is_err()
		);

		// The directory's project reuses the adopted entry.
		let allocation = pool
			.allocate(tg::sandbox::Resource::new(60, 60), Some(1))
			.unwrap();
		assert_eq!(allocation.limit(), 60);
	}

	#[test]
	fn a_rejected_reuse_restores_existing_limits() {
		let pool = Pool::new(100, 0, 1);
		let stopped = pool
			.allocate(tg::sandbox::Resource::new(30, 30), Some(1))
			.unwrap();
		stopped
			.bind(control(
				Arc::new(AtomicU64::new(10)),
				Arc::new(AtomicU64::new(0)),
			))
			.unwrap();
		drop(stopped);
		let running = pool.allocate(100.into(), Some(2)).unwrap();
		let limit = Arc::new(AtomicU64::new(0));
		running
			.bind(control(Arc::new(AtomicU64::new(5)), limit.clone()))
			.unwrap();
		let before = running.limit();

		// A reuse that does not fit fails without clamping the running allocation's limit.
		assert!(
			pool.allocate(tg::sandbox::Resource::new(100, 100), Some(1))
				.is_err()
		);
		assert_eq!(running.limit(), before);
		assert_eq!(limit.load(Ordering::SeqCst), before);
	}

	#[test]
	fn a_forgotten_project_releases_its_minimum() {
		let pool = Pool::new(100, 0, 1);
		let stopped = pool
			.allocate(tg::sandbox::Resource::new(60, 60), Some(1))
			.unwrap();
		stopped
			.bind(control(
				Arc::new(AtomicU64::new(10)),
				Arc::new(AtomicU64::new(0)),
			))
			.unwrap();
		drop(stopped);
		assert!(
			pool.allocate(tg::sandbox::Resource::new(60, 60), Some(2))
				.is_err()
		);

		// Forgetting the project releases its minimum to other projects.
		pool.forget(1);
		assert!(
			pool.allocate(tg::sandbox::Resource::new(60, 60), Some(2))
				.is_ok()
		);
	}

	#[test]
	fn rejected_admission_restores_existing_limits() {
		let pool = Pool::new(100, 0, 1);
		let first = pool.allocate(100.into(), None).unwrap();
		let usage = Arc::new(AtomicU64::new(10));
		let limit = Arc::new(AtomicU64::new(0));
		first.bind(control(usage, limit.clone())).unwrap();
		assert!(
			pool.allocate(tg::sandbox::Resource::new(100, 100), None)
				.is_err()
		);
		assert_eq!(first.limit(), 100);
		assert_eq!(limit.load(Ordering::SeqCst), 100);
	}

	#[test]
	fn persistent_data_prevents_admitting_an_unbacked_minimum() {
		let pool = Pool::new(100, 0, 1);
		let first = pool.allocate(100.into(), None).unwrap();
		let usage = Arc::new(AtomicU64::new(90));
		let limit = Arc::new(AtomicU64::new(0));
		first.bind(control(usage.clone(), limit)).unwrap();
		assert!(
			pool.allocate(tg::sandbox::Resource::new(20, 20), None)
				.is_err()
		);
		drop(first);
		assert!(
			pool.allocate(tg::sandbox::Resource::new(20, 20), None)
				.is_err()
		);
		usage.store(0, Ordering::SeqCst);
		assert!(
			pool.allocate(tg::sandbox::Resource::new(20, 20), None)
				.is_ok()
		);
	}
}
