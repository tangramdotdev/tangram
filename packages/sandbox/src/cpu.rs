use {
	crate::container::cgroup::Handle,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
		time::{Duration, Instant},
	},
	tangram_client::prelude::*,
};

/// An empty, delegated, exclusive cpuset partition containing complete physical cores.
#[derive(Clone, Debug)]
pub struct Pool(Arc<State>);

/// A physical CPU reservation that can be lent without acquiring another pool slot.
#[derive(Clone, Debug)]
pub struct Lease(Arc<Reservation>);

#[derive(Debug)]
struct Reservation {
	// Keep the ancestor reservation alive until every borrowing lease is released.
	_parent: Option<Lease>,
	id: u64,
	pool: Pool,
}

#[derive(Debug)]
struct State {
	allocations: Mutex<Allocations>,
	cores: BTreeMap<u32, BTreeSet<u32>>,
	oversubscription: u64,
	parent: PathBuf,
}

#[derive(Debug, Default)]
struct Allocations {
	entries: BTreeMap<u64, Entry>,
	next_id: u64,
	poisoned: bool,
}

#[derive(Debug)]
struct Entry {
	cpu: tg::sandbox::Cpu,
	dedicated: BTreeSet<u32>,
	handle: Option<Handle>,
	parent: Option<u64>,
	shared: BTreeSet<u32>,
}

#[derive(Debug)]
pub(crate) struct Allocation {
	lease: Lease,
	shared: BTreeSet<u32>,
	started_at: Instant,
}

impl Pool {
	pub fn new(parent: PathBuf, oversubscription: u64) -> tg::Result<Self> {
		if oversubscription == 0 {
			return Err(tg::error!(
				"the CPU oversubscription factor must be greater than zero"
			));
		}
		// Validate the partition before assigning any cores.
		let partition = read(&parent.join("cpuset.cpus.partition"))?;
		if !matches!(partition.trim(), "root" | "isolated") {
			return Err(tg::error!(
				"the sandbox CPU pool must be a valid exclusive cpuset partition"
			));
		}
		if !read(&parent.join("cgroup.procs"))?.trim().is_empty() {
			return Err(tg::error!(
				"the sandbox CPU pool must not contain host processes"
			));
		}
		for entry in std::fs::read_dir(&parent)
			.map_err(|error| tg::error!(!error, "failed to inspect the sandbox CPU pool"))?
		{
			let entry = entry
				.map_err(|error| tg::error!(!error, "failed to inspect the sandbox CPU pool"))?;
			if entry
				.file_type()
				.map_err(|error| tg::error!(!error, "failed to inspect the sandbox CPU pool"))?
				.is_dir()
			{
				return Err(tg::error!(
					"the sandbox CPU pool must not contain existing child cgroups"
				));
			}
		}
		let cpus = parse_list(&read(&parent.join("cpuset.cpus.effective"))?)?;
		let exclusive = parse_list(&read(&parent.join("cpuset.cpus.exclusive.effective"))?)?;
		if cpus.is_empty() || cpus != exclusive {
			return Err(tg::error!(
				"the sandbox CPU pool must contain only exclusive CPUs"
			));
		}
		let controllers = read(&parent.join("cgroup.subtree_control"))?;
		for controller in ["cpu", "cpuset", "memory", "pids"] {
			if !controllers
				.split_ascii_whitespace()
				.any(|value| value == controller)
			{
				return Err(
					tg::error!(%controller, "the sandbox CPU pool requires an enabled controller"),
				);
			}
		}
		let mut cores = BTreeMap::new();
		for cpu in &cpus {
			let path = PathBuf::from(format!(
				"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
			));
			let siblings = parse_list(&read(&path)?)?;
			if !siblings.is_subset(&cpus) {
				return Err(
					tg::error!(%cpu, "all SMT siblings of each core must belong to the sandbox CPU pool"),
				);
			}
			let core = *siblings
				.first()
				.ok_or_else(|| tg::error!("the CPU has no SMT siblings"))?;
			cores.insert(core, siblings);
		}
		u64::try_from(cores.len())
			.unwrap()
			.checked_mul(oversubscription)
			.ok_or_else(|| tg::error!("the shared CPU capacity is too large"))?;
		let state = State {
			allocations: Mutex::new(Allocations::default()),
			cores,
			oversubscription,
			parent,
		};
		Ok(Self(Arc::new(state)))
	}

	#[must_use]
	pub fn capacity(&self) -> tg::runner::Capacity {
		let dedicated = self.0.cores.len().try_into().unwrap();
		let cpu = tg::sandbox::Cpu {
			dedicated,
			shared: 0,
		};
		tg::runner::Capacity { cpu, memory: 0 }
	}

	pub(crate) fn allocate(&self, cpu: tg::sandbox::Cpu) -> tg::Result<Allocation> {
		cpu.validate()?;
		let mut allocations = self.0.allocations.lock().unwrap();
		if allocations.poisoned {
			return Err(tg::error!(
				"the CPU pool is unavailable after a failed reassignment"
			));
		}
		let reserved: BTreeSet<_> = allocations
			.entries
			.values()
			.filter(|entry| entry.parent.is_none())
			.flat_map(|entry| entry.dedicated.iter().copied())
			.collect();
		let count = usize::try_from(cpu.dedicated)
			.map_err(|_| tg::error!("the dedicated CPU request is too large"))?;
		let dedicated: BTreeSet<_> = self
			.0
			.cores
			.keys()
			.filter(|core| !reserved.contains(core))
			.take(count)
			.copied()
			.collect();
		if dedicated.len() != count {
			return Err(tg::error!("there are not enough available dedicated cores"));
		}
		let shared = self
			.0
			.cores
			.keys()
			.filter(|core| !dedicated.contains(core))
			.copied()
			.collect();
		let id = allocations.next_id;
		allocations.next_id = allocations
			.next_id
			.checked_add(1)
			.ok_or_else(|| tg::error!("the CPU allocation ID overflowed"))?;
		let entry = Entry {
			cpu,
			dedicated,
			handle: None,
			parent: None,
			shared: BTreeSet::new(),
		};
		allocations.entries.insert(id, entry);
		if let Err(error) = self.reassign(&mut allocations) {
			allocations.entries.remove(&id);
			return Err(error);
		}
		let reservation = Reservation {
			_parent: None,
			id,
			pool: self.clone(),
		};
		let lease = Lease(Arc::new(reservation));
		Ok(Allocation {
			lease,
			shared,
			started_at: Instant::now(),
		})
	}

	pub(crate) fn borrow(&self, parent: &Lease, cpu: tg::sandbox::Cpu) -> tg::Result<Allocation> {
		cpu.validate()?;
		if !Arc::ptr_eq(&self.0, &parent.0.pool.0) {
			return Err(tg::error!("the parent CPU lease belongs to another pool"));
		}
		let mut allocations = self.0.allocations.lock().unwrap();
		if allocations.poisoned {
			return Err(tg::error!(
				"the CPU pool is unavailable after a failed reassignment"
			));
		}
		let entry = allocations
			.entries
			.get(&parent.0.id)
			.ok_or_else(|| tg::error!("the parent CPU reservation is missing"))?;
		let capacity = tg::runner::Capacity {
			cpu: entry.cpu,
			memory: 0,
		};
		let requested = tg::runner::Capacity { cpu, memory: 0 };
		if !capacity.contains(requested, self.0.oversubscription) {
			return Err(tg::error!("the CPU request exceeds the parent reservation"));
		}
		if allocations
			.entries
			.values()
			.any(|entry| entry.parent == Some(parent.0.id))
		{
			return Err(tg::error!("the parent CPU reservation is already borrowed"));
		}
		let count = usize::try_from(cpu.dedicated)
			.map_err(|_| tg::error!("the dedicated CPU request is too large"))?;
		let dedicated: BTreeSet<_> = entry.dedicated.iter().take(count).copied().collect();
		let shared = self
			.0
			.cores
			.keys()
			.filter(|core| !dedicated.contains(core))
			.copied()
			.collect();
		let id = allocations.next_id;
		allocations.next_id = allocations
			.next_id
			.checked_add(1)
			.ok_or_else(|| tg::error!("the CPU allocation ID overflowed"))?;
		let entry = Entry {
			cpu,
			dedicated,
			handle: None,
			parent: Some(parent.0.id),
			shared: BTreeSet::new(),
		};
		allocations.entries.insert(id, entry);
		if let Err(error) = self.reassign(&mut allocations) {
			allocations.entries.remove(&id);
			return Err(error);
		}
		let reservation = Reservation {
			_parent: Some(parent.clone()),
			id,
			pool: self.clone(),
		};
		let lease = Lease(Arc::new(reservation));
		Ok(Allocation {
			lease,
			shared,
			started_at: Instant::now(),
		})
	}

	fn reassign(&self, allocations: &mut Allocations) -> tg::Result<()> {
		let assignments = self.assignments(allocations)?;
		Self::apply(allocations, assignments)?;
		Ok(())
	}

	fn assignments(&self, allocations: &Allocations) -> tg::Result<BTreeMap<u64, BTreeSet<u32>>> {
		// Pack shared requests onto as few cores as their parallelism and sharing limit permit.
		let reserved: BTreeSet<_> = allocations
			.entries
			.values()
			.filter(|entry| entry.parent.is_none())
			.flat_map(|entry| entry.dedicated.iter().copied())
			.collect();
		let shared = allocations
			.entries
			.values()
			.filter(|entry| entry.parent.is_none())
			.try_fold(0_u64, |sum, entry| sum.checked_add(entry.cpu.shared))
			.ok_or_else(|| tg::error!("the shared CPU capacity overflowed"))?;
		let width = allocations
			.entries
			.values()
			.filter(|entry| entry.parent.is_none())
			.map(|entry| entry.cpu.shared)
			.max()
			.unwrap_or(0);
		let count = shared.div_ceil(self.0.oversubscription).max(width);
		let count = usize::try_from(count)
			.map_err(|_| tg::error!("the shared CPU request is too large"))?;
		let cores: Vec<_> = self
			.0
			.cores
			.keys()
			.filter(|core| !reserved.contains(core))
			.take(count)
			.copied()
			.collect();
		if cores.len() != count {
			return Err(tg::error!(
				"there are not enough cores for the shared CPU requests"
			));
		}
		let mut loads: BTreeMap<_, u64> = cores.into_iter().map(|core| (core, 0)).collect();
		let mut assignments = BTreeMap::new();
		for (id, entry) in &allocations.entries {
			if let Some(parent) = entry.parent {
				let parent_entry = &allocations.entries[&parent];
				let parent_shared: &BTreeSet<u32> = &assignments[&parent];
				let count = usize::try_from(entry.cpu.shared)
					.map_err(|_| tg::error!("the shared CPU request is too large"))?;
				let mut shared: BTreeSet<_> = parent_shared.iter().take(count).copied().collect();
				let converted = entry
					.cpu
					.shared
					.saturating_sub(parent_entry.cpu.shared)
					.div_ceil(self.0.oversubscription);
				let converted = usize::try_from(converted)
					.map_err(|_| tg::error!("the shared CPU request is too large"))?;
				shared.extend(
					parent_entry
						.dedicated
						.difference(&entry.dedicated)
						.take(converted)
						.copied(),
				);
				assignments.insert(*id, shared);
				continue;
			}

			let mut cores: Vec<_> = loads.iter().map(|(core, load)| (*load, *core)).collect();
			cores.sort_unstable();
			let count = usize::try_from(entry.cpu.shared)
				.map_err(|_| tg::error!("the shared CPU request is too large"))?;
			let shared: BTreeSet<_> = cores
				.into_iter()
				.take(count)
				.map(|(_, core)| core)
				.collect();
			for core in &shared {
				let load = loads.get_mut(core).unwrap();
				*load += 1;
				if *load > self.0.oversubscription {
					return Err(tg::error!(
						"the shared CPU oversubscription limit was exceeded"
					));
				}
			}
			assignments.insert(*id, shared);
		}

		Ok(assignments)
	}

	fn apply(
		allocations: &mut Allocations,
		assignments: BTreeMap<u64, BTreeSet<u32>>,
	) -> tg::Result<()> {
		// Freeze affected cgroups before moving them, and roll back every changed CPU set on failure.
		let changed: Vec<_> = allocations
			.entries
			.iter()
			.filter(|(id, entry)| entry.handle.is_some() && entry.shared != assignments[id])
			.map(|(id, _)| *id)
			.collect();
		let mut frozen = Vec::new();
		let result = (|| {
			for id in &changed {
				let handle = allocations.entries[id].handle.as_ref().unwrap();
				handle.write(c"cgroup.freeze", b"1")?;
				frozen.push(*id);
				let deadline = Instant::now() + Duration::from_secs(5);
				while !handle
					.read(c"cgroup.events")?
					.lines()
					.any(|line| line == "frozen 1")
				{
					if Instant::now() >= deadline {
						return Err(tg::error!(
							"timed out freezing a sandbox for CPU reassignment"
						));
					}
					std::thread::sleep(Duration::from_millis(1));
				}
			}
			for id in &changed {
				let entry = &allocations.entries[id];
				let cpus = entry.dedicated.union(&assignments[id]).copied().collect();
				entry
					.handle
					.as_ref()
					.unwrap()
					.write(c"cpuset.cpus", format_list(&cpus).as_bytes())?;
			}
			Ok(())
		})();
		if result.is_err() {
			for id in &changed {
				let entry = &allocations.entries[id];
				let cpus = entry.dedicated.union(&entry.shared).copied().collect();
				if let Err(error) = entry
					.handle
					.as_ref()
					.unwrap()
					.write(c"cpuset.cpus", format_list(&cpus).as_bytes())
				{
					allocations.poisoned = true;
					tracing::error!(%error, "failed to restore a sandbox CPU assignment");
				}
			}
		} else {
			for (id, shared) in assignments {
				allocations.entries.get_mut(&id).unwrap().shared = shared;
			}
		}
		let mut thaw_error = None;
		for id in frozen {
			if let Err(error) = allocations.entries[&id]
				.handle
				.as_ref()
				.unwrap()
				.write(c"cgroup.freeze", b"0")
			{
				allocations.poisoned = true;
				thaw_error.get_or_insert(error);
			}
		}
		if let Some(error) = thaw_error {
			return Err(error);
		}
		result
	}
}

impl Allocation {
	pub(crate) fn bind(&self, handle: Handle) -> tg::Result<()> {
		let mut allocations = self.lease.0.pool.0.allocations.lock().unwrap();
		let entry = allocations.entries.get_mut(&self.lease.0.id).unwrap();
		let cpus = entry.dedicated.union(&entry.shared).copied().collect();
		handle.write(c"cpuset.cpus", format_list(&cpus).as_bytes())?;
		entry.handle = Some(handle);
		Ok(())
	}

	#[must_use]
	pub(crate) fn lease(&self) -> Lease {
		self.lease.clone()
	}

	#[must_use]
	pub(crate) fn parent(&self) -> &Path {
		&self.lease.0.pool.0.parent
	}

	/// Monitor every potential shared core so reassignment preserves mixed CPU accounting.
	#[must_use]
	pub(crate) fn shared(&self) -> &BTreeSet<u32> {
		&self.shared
	}

	#[must_use]
	pub(crate) fn started_at(&self) -> Instant {
		self.started_at
	}
}

impl Drop for Allocation {
	fn drop(&mut self) {
		let mut allocations = self.lease.0.pool.0.allocations.lock().unwrap();
		if let Some(entry) = allocations.entries.get_mut(&self.lease.0.id) {
			entry.handle = None;
		}
	}
}

impl Drop for Reservation {
	fn drop(&mut self) {
		let mut allocations = self.pool.0.allocations.lock().unwrap();
		allocations.entries.remove(&self.id);
		if let Err(error) = self.pool.reassign(&mut allocations) {
			allocations.poisoned = true;
			tracing::error!(%error, "failed to consolidate the shared CPU allocations");
		}
	}
}

pub(crate) fn parse_list(value: &str) -> tg::Result<BTreeSet<u32>> {
	let mut cpus = BTreeSet::new();
	for part in value.trim().split(',').filter(|part| !part.is_empty()) {
		let (start, end) = part.split_once('-').unwrap_or((part, part));
		let start: u32 = start
			.parse()
			.map_err(|error| tg::error!(!error, "invalid CPU list"))?;
		let end: u32 = end
			.parse()
			.map_err(|error| tg::error!(!error, "invalid CPU list"))?;
		if start > end {
			return Err(tg::error!("invalid CPU range"));
		}
		cpus.extend(start..=end);
	}
	Ok(cpus)
}

pub(crate) fn format_list(cpus: &BTreeSet<u32>) -> String {
	cpus.iter()
		.map(u32::to_string)
		.collect::<Vec<_>>()
		.join(",")
}

fn read(path: &Path) -> tg::Result<String> {
	let value = std::fs::read_to_string(path).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to read the CPU pool configuration"),
	)?;
	Ok(value)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pool(cores: u32, oversubscription: u64) -> Pool {
		let cores = (0..cores)
			.map(|core| (core, BTreeSet::from([core, core + 8])))
			.collect();
		let state = State {
			allocations: Mutex::new(Allocations::default()),
			cores,
			oversubscription,
			parent: PathBuf::new(),
		};
		Pool(Arc::new(state))
	}

	#[test]
	fn borrowers_share_the_reservation_and_keep_it_alive_after_parent_cleanup() {
		let pool = pool(1, 4);
		let cpu = tg::sandbox::Cpu {
			dedicated: 1,
			shared: 0,
		};
		let parent = pool.allocate(cpu).unwrap();
		let lease = parent.lease();
		let child = pool.borrow(&lease, 4.into()).unwrap();
		let child_lease = child.lease();
		{
			let allocations = pool.0.allocations.lock().unwrap();
			assert_eq!(
				allocations.entries[&child.lease.0.id].shared,
				BTreeSet::from([0])
			);
		}
		assert!(pool.borrow(&child_lease, cpu).is_err());
		let grandchild = pool.borrow(&child_lease, 4.into()).unwrap();
		drop(parent);
		drop(lease);
		drop(child);
		drop(child_lease);
		assert!(pool.allocate(cpu).is_err());
		drop(grandchild);
		assert!(pool.allocate(cpu).is_ok());
	}

	#[test]
	fn shared_borrowers_follow_parent_reassignment() {
		let pool = pool(2, 4);
		let parent = pool.allocate(1.into()).unwrap();
		let lease = parent.lease();
		let child = pool.borrow(&lease, 1.into()).unwrap();
		assert!(pool.borrow(&lease, 1.into()).is_err());
		let cpu = tg::sandbox::Cpu {
			dedicated: 1,
			shared: 0,
		};
		let dedicated = pool.allocate(cpu).unwrap();
		{
			let allocations = pool.0.allocations.lock().unwrap();
			assert_eq!(
				allocations.entries[&parent.lease.0.id].shared,
				BTreeSet::from([1])
			);
			assert_eq!(
				allocations.entries[&child.lease.0.id].shared,
				BTreeSet::from([1])
			);
		}
		drop(dedicated);
		let allocations = pool.0.allocations.lock().unwrap();
		assert_eq!(
			allocations.entries[&parent.lease.0.id].shared,
			BTreeSet::from([0])
		);
		assert_eq!(
			allocations.entries[&child.lease.0.id].shared,
			BTreeSet::from([0])
		);
	}

	#[test]
	fn packs_shared_allocations_and_moves_them_for_dedicated_cores() {
		let pool = pool(3, 4);
		let shared: Vec<_> = (0..4).map(|_| pool.allocate(1.into()).unwrap()).collect();
		{
			let allocations = pool.0.allocations.lock().unwrap();
			assert!(
				allocations
					.entries
					.values()
					.all(|entry| entry.shared == BTreeSet::from([0]))
			);
		}
		let cpu = tg::sandbox::Cpu {
			dedicated: 2,
			shared: 0,
		};
		let dedicated = pool.allocate(cpu).unwrap();
		{
			let allocations = pool.0.allocations.lock().unwrap();
			assert_eq!(
				allocations.entries[&dedicated.lease.0.id].dedicated,
				BTreeSet::from([0, 1])
			);
			assert!(shared.iter().all(|allocation| {
				allocations.entries[&allocation.lease.0.id].shared == BTreeSet::from([2])
			}));
		}
		assert!(pool.allocate(1.into()).is_err());
		drop(dedicated);
		let allocations = pool.0.allocations.lock().unwrap();
		assert!(
			allocations
				.entries
				.values()
				.all(|entry| entry.shared == BTreeSet::from([0]))
		);
	}

	#[test]
	fn preserves_parallelism_and_rolls_back_unplaceable_requests() {
		let pool = pool(3, 4);
		let shared = pool.allocate(2.into()).unwrap();
		let cpu = tg::sandbox::Cpu {
			dedicated: 2,
			shared: 0,
		};
		assert!(pool.allocate(cpu).is_err());
		{
			let allocations = pool.0.allocations.lock().unwrap();
			assert_eq!(allocations.entries.len(), 1);
			assert_eq!(
				allocations.entries[&shared.lease.0.id].shared,
				BTreeSet::from([0, 1])
			);
		}
		let cpu = tg::sandbox::Cpu {
			dedicated: 1,
			shared: 2,
		};
		let mixed = pool.allocate(cpu).unwrap();
		assert!(mixed.shared.is_disjoint(&BTreeSet::from([0])));
		let allocations = pool.0.allocations.lock().unwrap();
		assert_eq!(
			allocations.entries[&mixed.lease.0.id].shared,
			BTreeSet::from([1, 2])
		);
	}

	#[test]
	fn lists() {
		assert_eq!(
			parse_list("0-2,4,6-7\n").unwrap(),
			BTreeSet::from([0, 1, 2, 4, 6, 7])
		);
		assert!(parse_list("2-0").is_err());
		assert!(parse_list("a").is_err());
		assert_eq!(format_list(&BTreeSet::from([1, 3])), "1,3");
	}
}
