use {
	crate::cgroup::Handle,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Pool(Arc<State>);

#[derive(Clone, Debug)]
pub struct Lease(Arc<Reservation>);

#[derive(Debug)]
struct Reservation {
	_parent: Option<Lease>,
	id: u64,
	pool: Pool,
}

#[derive(Debug)]
struct State {
	allocations: Mutex<Allocations>,
	cores: BTreeMap<u32, BTreeSet<u32>>,
	cpu_overcommit: u64,
	parent: PathBuf,
	threads_per_core: u64,
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
	cpus: BTreeSet<u32>,
	handle: Option<Handle>,
	parent: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct Allocation {
	lease: Lease,
}

pub(crate) fn threads(cpu: tg::sandbox::Cpu, _pool: Option<&Pool>) -> tg::Result<u64> {
	cpu.total()
}

impl Pool {
	pub fn new(parent: PathBuf, cpu_overcommit: u64) -> tg::Result<Self> {
		// Validate the overcommit setting and parent partition.
		if cpu_overcommit == 0 {
			return Err(tg::error!("the CPU overcommit must be greater than zero"));
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

		// Validate the exclusive CPU set.
		let cpus = parse_list(&read(&parent.join("cpuset.cpus.effective"))?)?;
		let exclusive = parse_list(&read(&parent.join("cpuset.cpus.exclusive.effective"))?)?;
		if cpus.is_empty() || cpus != exclusive {
			return Err(tg::error!(
				"the sandbox CPU pool must contain only exclusive CPUs"
			));
		}

		// Validate the resource controllers and parent quota.
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
		for ancestor in parent
			.ancestors()
			.take_while(|ancestor| ancestor.starts_with("/sys/fs/cgroup"))
		{
			let Ok(limit) = std::fs::read_to_string(ancestor.join("cpu.max")) else {
				continue;
			};
			let mut limit = limit.split_ascii_whitespace();
			let Some(quota) = limit.next() else {
				return Err(tg::error!("the CPU budget is missing"));
			};
			if quota == "max" {
				continue;
			}
			let quota = quota
				.parse::<u64>()
				.map_err(|error| tg::error!(!error, "invalid ancestor CPU quota"))?;
			let period = limit
				.next()
				.ok_or_else(|| tg::error!("the ancestor CPU period is missing"))?
				.parse::<u64>()
				.map_err(|error| tg::error!(!error, "invalid ancestor CPU period"))?;
			if u128::from(quota) < cpus.len() as u128 * u128::from(period) {
				return Err(tg::error!(
					"the CPU pool exceeds its ancestor execution budget"
				));
			}
		}

		// Collect the CPU topology.
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

		// Validate the thread counts and capacity.
		let threads_per_core = u64::try_from(cores.values().next().unwrap().len()).unwrap();
		if cores
			.values()
			.any(|siblings| u64::try_from(siblings.len()).unwrap() != threads_per_core)
		{
			return Err(tg::error!(
				"the CPU pool requires the same number of hardware threads per core"
			));
		}
		u64::try_from(cpus.len())
			.unwrap()
			.checked_mul(cpu_overcommit)
			.and_then(|capacity| capacity.checked_mul(1000))
			.filter(|capacity| u32::try_from(*capacity).is_ok())
			.ok_or_else(|| tg::error!("the CPU capacity is too large"))?;

		// Create the CPU pool state.
		let state = State {
			allocations: Mutex::new(Allocations::default()),
			cores,
			cpu_overcommit,
			parent,
			threads_per_core,
		};

		Ok(Self(Arc::new(state)))
	}

	pub fn budget(&self, memory: u64, swap: u64) -> tg::Result<()> {
		for (name, value) in [
			("memory.max", memory),
			("memory.min", memory),
			("memory.swap.max", swap),
		] {
			std::fs::write(self.0.parent.join(name), value.to_string()).map_err(|error| {
				tg::error!(!error, "failed to configure the runner resource budget")
			})?;
		}
		Ok(())
	}

	#[must_use]
	pub fn capacity(&self) -> tg::runner::Capacity {
		let threads: u64 = self.0.cores.values().map(|cpus| cpus.len() as u64).sum();
		let min = tg::sandbox::Amount::from_millis(threads * 1000);
		let max = tg::sandbox::Amount::from_millis(threads * 1000 * self.0.cpu_overcommit);
		let cpu = tg::sandbox::Cpu { max, min };
		tg::runner::Capacity {
			cpu,
			..Default::default()
		}
	}

	#[must_use]
	pub fn available(&self) -> tg::sandbox::Cpu {
		let total = self.capacity().cpu;
		let entries = self.0.allocations.lock().unwrap();
		let (minimum, cost) = entries
			.entries
			.values()
			.filter(|entry| entry.parent.is_none())
			.fold((0_u64, 0_u128), |(minimum, cost), entry| {
				let reserved = reservation(entry.cpu, entry.cpus.len());
				let excess = entry.cpu.max.millis().saturating_sub(reserved);
				(
					minimum.saturating_add(reserved),
					cost + u128::from(reserved) * u128::from(self.0.cpu_overcommit)
						+ u128::from(excess),
				)
			});
		let max =
			u64::try_from(u128::from(total.max.millis()).saturating_sub(cost)).unwrap_or_default();
		let min = total
			.min
			.millis()
			.saturating_sub(minimum)
			.min(max / self.0.cpu_overcommit);
		let max = tg::sandbox::Amount::from_millis(max);
		let min = tg::sandbox::Amount::from_millis(min);
		tg::sandbox::Cpu { max, min }
	}

	#[must_use]
	pub fn threads_per_core(&self) -> u64 {
		self.0.threads_per_core
	}

	#[must_use]
	pub fn healthy(&self) -> bool {
		!self.0.allocations.lock().unwrap().poisoned
	}

	pub(crate) fn allocate(&self, cpu: tg::sandbox::Cpu) -> tg::Result<Allocation> {
		// Validate the CPU request and lock the allocations.
		cpu.total()?;
		let mut allocations = self.0.allocations.lock().unwrap();
		if allocations.poisoned {
			return Err(tg::error!(
				"the CPU pool is unavailable after a failed update"
			));
		}

		// Collect the occupied CPU domains.
		let width = usize::try_from(cpu.max.threads())
			.map_err(|_| tg::error!("the CPU request is too large"))?;
		let mut domains = BTreeSet::new();
		let mut occupied: BTreeSet<u32> = BTreeSet::new();
		for entry in allocations
			.entries
			.values()
			.filter(|entry| entry.parent.is_none())
		{
			occupied.extend(entry.cpus.iter().copied());
			if entry.cpus.len() == width {
				domains.insert(entry.cpus.clone());
			}
		}

		// Select the available CPUs.
		let free: BTreeSet<_> = self
			.0
			.cores
			.values()
			.flatten()
			.filter(|cpu| !occupied.contains(cpu))
			.take(width)
			.copied()
			.collect();
		if free.len() == width {
			domains.insert(free);
		}

		// Select a shared CPU domain when no free domain is available.
		let cpus = domains
			.into_iter()
			.find(|cpus| {
				let entries = allocations
					.entries
					.values()
					.filter(|entry| entry.parent.is_none() && entry.cpus == *cpus);
				let (min, excess) = entries.fold(
					(
						u128::from(reservation(cpu, width)),
						u128::from(cpu.max.millis().saturating_sub(reservation(cpu, width))),
					),
					|(min, excess), entry| {
						(
							min + u128::from(reservation(entry.cpu, entry.cpus.len())),
							excess
								+ u128::from(
									entry
										.cpu
										.max
										.millis()
										.saturating_sub(reservation(entry.cpu, entry.cpus.len())),
								),
						)
					},
				);
				let capacity = cpus.len() as u128 * 1000;
				min <= capacity && excess <= (capacity - min) * u128::from(self.0.cpu_overcommit)
			})
			.ok_or_else(|| tg::error!("there is no available CPU affinity domain"))?;

		self.insert(&mut allocations, cpu, cpus, None)
	}

	pub(crate) fn borrow(&self, parent: &Lease, cpu: tg::sandbox::Cpu) -> tg::Result<Allocation> {
		cpu.total()?;
		if !Arc::ptr_eq(&self.0, &parent.0.pool.0) {
			return Err(tg::error!("the CPU reservation belongs to another pool"));
		}
		let mut allocations = self.0.allocations.lock().unwrap();
		let entry = allocations
			.entries
			.get(&parent.0.id)
			.ok_or_else(|| tg::error!("the CPU reservation is missing"))?;
		if cpu.min > entry.cpu.min || cpu.max > entry.cpu.max {
			return Err(tg::error!("the CPU request exceeds the reservation"));
		}
		let cpus = entry.cpus.clone();
		self.insert(&mut allocations, cpu, cpus, Some(parent.clone()))
	}

	fn insert(
		&self,
		allocations: &mut Allocations,
		cpu: tg::sandbox::Cpu,
		cpus: BTreeSet<u32>,
		parent: Option<Lease>,
	) -> tg::Result<Allocation> {
		let id = allocations.next_id;
		allocations.next_id += 1;
		let entry = Entry {
			cpu,
			cpus,
			handle: None,
			parent: parent.as_ref().map(|parent| parent.0.id),
		};
		allocations.entries.insert(id, entry);
		if let Err(error) = Self::weights(allocations) {
			allocations.entries.remove(&id);
			if Self::weights(allocations).is_err() {
				allocations.poisoned = true;
			}
			return Err(error);
		}
		let reservation = Reservation {
			_parent: parent,
			id,
			pool: self.clone(),
		};
		let lease = Lease(Arc::new(reservation));
		Ok(Allocation { lease })
	}

	fn weights(allocations: &Allocations) -> tg::Result<()> {
		let domains: BTreeSet<_> = allocations
			.entries
			.values()
			.map(|entry| entry.cpus.clone())
			.collect();
		for domain in domains {
			let entries: Vec<_> = allocations
				.entries
				.iter()
				.filter(|(_, entry)| entry.cpus == domain)
				.collect();
			let capacity = domain.len() as u128 * 1000;
			let weights: Vec<_> = entries
				.iter()
				.map(|(_, entry)| {
					(u128::from(reservation(entry.cpu, entry.cpus.len())) * 10_000)
						.div_ceil(capacity)
						.max(1)
				})
				.collect();
			let reserved: u128 = weights.iter().sum();
			if reserved > 10_000 {
				return Err(tg::error!(
					"the CPU reservation exceeds the scheduler weight precision"
				));
			}
			let excess: u128 = entries
				.iter()
				.map(|(_, entry)| {
					u128::from(
						entry
							.cpu
							.max
							.millis()
							.saturating_sub(reservation(entry.cpu, entry.cpus.len())),
					)
				})
				.sum();
			for ((_, entry), weight) in entries.into_iter().zip(weights) {
				let weight = weight
					+ (10_000 - reserved)
						* u128::from(
							entry
								.cpu
								.max
								.millis()
								.saturating_sub(reservation(entry.cpu, entry.cpus.len())),
						) / excess.max(1);
				if let Some(handle) = &entry.handle {
					handle.write(c"cpu.weight", weight.to_string().as_bytes())?;
				}
			}
		}
		Ok(())
	}
}

impl Allocation {
	pub(crate) fn bind(&self, handle: Handle) -> tg::Result<()> {
		let mut allocations = self.lease.0.pool.0.allocations.lock().unwrap();
		let entry = allocations.entries.get_mut(&self.lease.0.id).unwrap();
		handle.write(c"cpuset.cpus", format_list(&entry.cpus).as_bytes())?;
		entry.handle = Some(handle);
		Pool::weights(&allocations)?;
		Ok(())
	}

	pub(crate) fn retain(self) {
		self.lease.0.pool.0.allocations.lock().unwrap().poisoned = true;
		std::mem::forget(self);
	}

	#[must_use]
	pub(crate) fn lease(&self) -> Lease {
		self.lease.clone()
	}

	#[must_use]
	pub(crate) fn parent(&self) -> &Path {
		&self.lease.0.pool.0.parent
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
		if let Err(error) = Pool::weights(&allocations) {
			allocations.poisoned = true;
			tracing::error!(%error, "failed to update the CPU reservations");
		}
	}
}
fn reservation(cpu: tg::sandbox::Cpu, width: usize) -> u64 {
	cpu.min.millis().min(1000) * u64::try_from(width).unwrap()
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

	fn pool() -> Pool {
		let cores = BTreeMap::from([(0, BTreeSet::from([0, 1])), (2, BTreeSet::from([2, 3]))]);
		let state = State {
			allocations: Mutex::new(Allocations::default()),
			cores,
			cpu_overcommit: 4,
			parent: PathBuf::new(),
			threads_per_core: 2,
		};
		Pool(Arc::new(state))
	}

	fn cpu(min: u64, max: u64) -> tg::sandbox::Cpu {
		tg::sandbox::Cpu::new(
			tg::sandbox::Amount::from_millis(min),
			tg::sandbox::Amount::from_millis(max),
		)
	}

	#[test]
	fn fractional_reservations_keep_fixed_affinity_and_include_smt_threads() {
		let pool = pool();
		assert_eq!(pool.capacity().cpu.min.millis(), 4000);
		let first = pool.allocate(cpu(250, 1000)).unwrap();
		let affinity = pool.0.allocations.lock().unwrap().entries[&first.lease.0.id]
			.cpus
			.clone();
		let others: Vec<_> = (0..3)
			.map(|_| pool.allocate(cpu(250, 1000)).unwrap())
			.collect();
		assert_eq!(
			pool.0.allocations.lock().unwrap().entries[&first.lease.0.id].cpus,
			affinity
		);
		let entries = pool.0.allocations.lock().unwrap();
		let minima: u64 = entries
			.entries
			.values()
			.filter(|entry| entry.cpus == affinity)
			.map(|entry| entry.cpu.min.millis())
			.sum();
		assert!(minima <= 1000);
		drop(entries);
		drop(others);
		assert_eq!(
			pool.0.allocations.lock().unwrap().entries[&first.lease.0.id].cpus,
			affinity
		);
	}

	#[test]
	fn multithread_affinity_accounts_for_minimum_on_each_thread() {
		let pool = pool();
		let allocation = pool.allocate(cpu(250, 2000)).unwrap();
		assert_eq!(pool.available().min.millis(), 3125);
		assert_eq!(pool.available().max.millis(), 12500);
		drop(allocation);
		assert_eq!(pool.available().min.millis(), 4000);
	}

	#[test]
	fn borrowers_and_lenders_both_participate_in_scheduler_weights() {
		use std::os::fd::IntoRawFd as _;
		let pool = pool();
		let reserved = pool.allocate(cpu(250, 250)).unwrap();
		let parent = pool.allocate(cpu(0, 1000)).unwrap();
		let child = pool.borrow(&parent.lease(), cpu(0, 1000)).unwrap();
		let temp = tangram_util::fs::Temp::new().unwrap();
		let mut weights = Vec::new();
		for (index, allocation) in [&reserved, &parent, &child].into_iter().enumerate() {
			let path = temp.path().join(index.to_string());
			std::fs::create_dir_all(&path).unwrap();
			std::fs::write(path.join("cpu.weight"), "").unwrap();
			std::fs::write(path.join("cpuset.cpus"), "").unwrap();
			let directory = std::fs::File::open(&path).unwrap().into_raw_fd();

			// SAFETY: The file descriptor is owned and refers to the test directory.
			let handle = unsafe { Handle::from_raw_fd(directory) };
			allocation.bind(handle).unwrap();
			weights.push(
				std::fs::read_to_string(path.join("cpu.weight"))
					.unwrap()
					.parse::<u64>()
					.unwrap(),
			);
		}
		assert_eq!(weights, [2500, 3750, 3750]);
		assert_eq!(weights.iter().sum::<u64>(), 10_000);
	}

	#[test]
	fn full_minima_leave_no_capacity_for_additional_cpu_maxima() {
		let pool = pool();
		let allocations: Vec<_> = (0..4)
			.map(|_| pool.allocate(cpu(1000, 1000)).unwrap())
			.collect();
		assert!(pool.allocate(cpu(0, 1000)).is_err());
		drop(allocations);
		assert!(pool.allocate(cpu(0, 1000)).is_ok());
	}
}
