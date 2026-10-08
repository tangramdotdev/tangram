use {
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Path, PathBuf},
		sync::{
			Arc, Mutex,
			atomic::{AtomicUsize, Ordering},
		},
		time::Instant,
	},
	tangram_client::prelude::*,
};

/// An empty, delegated, exclusive cpuset partition containing the sandbox CPUs.
#[derive(Clone, Debug)]
pub struct Pool(Arc<State>);

#[derive(Debug)]
struct State {
	available: Mutex<BTreeSet<u32>>,
	cores: BTreeMap<u32, BTreeSet<u32>>,
	next_shared: AtomicUsize,
	parent: PathBuf,
	shared: BTreeSet<u32>,
}

#[derive(Debug)]
pub(crate) struct Allocation {
	dedicated: BTreeSet<u32>,
	pool: Pool,
	shared: BTreeSet<u32>,
	started_at: Instant,
}

impl Pool {
	pub fn new(parent: PathBuf, dedicated: &[u32]) -> tg::Result<Self> {
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
		let entries = std::fs::read_dir(&parent)
			.map_err(|error| tg::error!(!error, "failed to inspect the sandbox CPU pool"))?;
		for entry in entries {
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
		let mut shared = cpus.clone();
		for cpu in dedicated {
			let path = PathBuf::from(format!(
				"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
			));
			let siblings = parse_list(&read(&path)?)?;
			if !siblings.is_subset(&cpus) {
				return Err(
					tg::error!(%cpu, "all SMT siblings of a dedicated core must belong to the sandbox CPU pool"),
				);
			}
			let core = *siblings
				.first()
				.ok_or_else(|| tg::error!("the CPU has no SMT siblings"))?;
			if cores.insert(core, siblings.clone()).is_some() {
				return Err(
					tg::error!(%cpu, "the dedicated CPU list contains the same physical core more than once"),
				);
			}
			shared = shared.difference(&siblings).copied().collect();
		}
		let available = Mutex::new(cores.keys().copied().collect());
		let state = State {
			available,
			cores,
			next_shared: AtomicUsize::new(0),
			parent,
			shared,
		};
		Ok(Self(Arc::new(state)))
	}

	#[must_use]
	pub fn capacity(&self) -> tg::runner::Capacity {
		tg::runner::Capacity {
			cpus: self.0.shared.len().try_into().unwrap(),
			dedicated_cpus: self.0.cores.len().try_into().unwrap(),
			memory: 0,
		}
	}

	pub(crate) fn allocate(&self, cpu: tg::sandbox::Cpu) -> tg::Result<Allocation> {
		cpu.validate()?;
		let count = usize::try_from(cpu.shared)
			.map_err(|_| tg::error!("the shared CPU limit is too large"))?;
		if count > self.0.shared.len() {
			return Err(tg::error!(
				"the shared CPU limit exceeds the sandbox CPU pool"
			));
		}
		let offset = if self.0.shared.is_empty() {
			0
		} else {
			self.0.next_shared.fetch_add(count, Ordering::Relaxed) % self.0.shared.len()
		};
		let shared = self
			.0
			.shared
			.iter()
			.cycle()
			.skip(offset)
			.take(count)
			.copied()
			.collect();
		let count = usize::try_from(cpu.dedicated)
			.map_err(|_| tg::error!("the dedicated CPU request is too large"))?;
		let mut available = self.0.available.lock().unwrap();
		if count > available.len() {
			return Err(tg::error!("there are not enough available dedicated cores"));
		}
		let dedicated: BTreeSet<_> = available.iter().take(count).copied().collect();
		for cpu in &dedicated {
			available.remove(cpu);
		}
		let pool = self.clone();
		Ok(Allocation {
			dedicated,
			pool,
			shared,
			started_at: Instant::now(),
		})
	}
}

impl Allocation {
	#[must_use]
	pub(crate) fn cpus(&self) -> BTreeSet<u32> {
		// Expose one hardware thread per physical core while reserving every SMT sibling.
		self.dedicated.union(&self.shared).copied().collect()
	}

	#[must_use]
	pub(crate) fn parent(&self) -> &Path {
		&self.pool.0.parent
	}

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
		self.pool
			.0
			.available
			.lock()
			.unwrap()
			.extend(&self.dedicated);
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
	#[test]
	fn dedicated_cores_remain_exclusive_until_released() {
		let state = State {
			available: Mutex::new(BTreeSet::from([0])),
			cores: BTreeMap::from([(0, BTreeSet::from([0, 4]))]),
			next_shared: AtomicUsize::new(0),
			parent: PathBuf::new(),
			shared: BTreeSet::from([1, 2]),
		};
		let pool = Pool(Arc::new(state));
		let cpu = tg::sandbox::Cpu {
			dedicated: 1,
			shared: 1,
		};
		let allocation = pool.allocate(cpu).unwrap();
		assert_eq!(allocation.cpus(), BTreeSet::from([0, 1]));
		assert!(pool.allocate(cpu).is_err());
		let shared = pool.allocate(1.into()).unwrap();
		assert!(shared.cpus().is_disjoint(&BTreeSet::from([0, 4])));
		drop(allocation);
		assert!(pool.allocate(cpu).is_ok());
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
