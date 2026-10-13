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
	orphaned: bool,
	resource: tg::sandbox::Resource<u64>,
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

	pub(crate) fn allocate(&self, resource: tg::sandbox::Resource<u64>) -> tg::Result<Allocation> {
		resource.validate()?;
		if resource.max < self.0.floor || self.round(resource.min)? > resource.max {
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
		let id = entries.next_id;
		entries.next_id += 1;
		let entry = Entry {
			control: None,
			limit: 0,
			orphaned: false,
			resource,
		};
		let limits: BTreeMap<_, _> = entries
			.entries
			.iter()
			.map(|(id, entry)| (*id, entry.limit))
			.collect();
		entries.entries.insert(id, entry);
		if let Err(error) = self.rebalance(&mut entries) {
			entries.entries.remove(&id);
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
			return Err(error);
		}
		Ok(Allocation {
			id,
			pool: self.clone(),
		})
	}

	fn rebalance(&self, entries: &mut Bookkeeping) -> tg::Result<()> {
		// Stop growth before measuring the capacity occupied by persistent data.
		for entry in entries.entries.values_mut() {
			if let Some(control) = &entry.control {
				let usage = (control.usage)()?;
				let floor = self.round(usage.max(entry.resource.min).max(self.0.floor))?;
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
			let floor = self.round(usage.max(entry.resource.min).max(self.0.floor))?;
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
			.map(|(id, entry)| {
				u128::from(entry.resource.max.saturating_sub(floors[id]) / self.0.unit)
			})
			.sum();
		for (id, entry) in &mut entries.entries {
			let floor = floors[id];
			let need = entry.resource.max.saturating_sub(floor) / self.0.unit;
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
		let first = pool.allocate(tg::sandbox::Resource::new(80, 100)).unwrap();
		let usage = Arc::new(AtomicU64::new(0));
		let limit = Arc::new(AtomicU64::new(0));
		first.bind(control(usage.clone(), limit.clone())).unwrap();
		let second = pool.allocate(100.into()).unwrap();
		assert!(limit.load(Ordering::SeqCst) >= 80);
		assert!(second.limit() <= 20);
		assert!(first.limit() + second.limit() <= 100);
	}

	#[test]
	fn rejected_admission_restores_existing_limits() {
		let pool = Pool::new(100, 0, 1);
		let first = pool.allocate(100.into()).unwrap();
		let usage = Arc::new(AtomicU64::new(10));
		let limit = Arc::new(AtomicU64::new(0));
		first.bind(control(usage, limit.clone())).unwrap();
		assert!(pool.allocate(tg::sandbox::Resource::new(100, 100)).is_err());
		assert_eq!(first.limit(), 100);
		assert_eq!(limit.load(Ordering::SeqCst), 100);
	}

	#[test]
	fn persistent_data_prevents_admitting_an_unbacked_minimum() {
		let pool = Pool::new(100, 0, 1);
		let first = pool.allocate(100.into()).unwrap();
		let usage = Arc::new(AtomicU64::new(90));
		let limit = Arc::new(AtomicU64::new(0));
		first.bind(control(usage.clone(), limit)).unwrap();
		assert!(pool.allocate(tg::sandbox::Resource::new(20, 20)).is_err());
		drop(first);
		assert!(pool.allocate(tg::sandbox::Resource::new(20, 20)).is_err());
		usage.store(0, Ordering::SeqCst);
		assert!(pool.allocate(tg::sandbox::Resource::new(20, 20)).is_ok());
	}
}
