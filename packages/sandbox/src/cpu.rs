use {
	crate::cgroup::Handle,
	std::{
		collections::BTreeMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

/// The CPU shares of a runner's sandboxes, which weight each sandbox by its minimum and divide the remaining weight in proportion to the capacity above each minimum.
#[derive(Clone, Debug)]
pub struct Shares(Arc<SharesState>);

#[derive(Debug)]
struct SharesState {
	capacity: u64,
	entries: Mutex<ShareEntries>,
}

#[derive(Debug, Default)]
struct ShareEntries {
	entries: BTreeMap<u64, ShareEntry>,
	next_id: u64,
}

#[derive(Debug)]
struct ShareEntry {
	cpu: tg::sandbox::Cpu,
	handle: Handle,
	lender: Option<ShareId>,
}

/// A sandbox's entry in the CPU shares, which leaves the shares when it is dropped.
#[derive(Debug)]
pub(crate) struct Share {
	id: ShareId,
	shares: Shares,
}

/// The identifier of a sandbox's entry in the CPU shares.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ShareId(u64);

/// The number of sandboxes pinned to each hardware thread of a runner, which pins each sandbox to the least used threads.
#[derive(Clone, Debug, Default)]
pub struct Threads(Arc<Mutex<BTreeMap<usize, u64>>>);

/// The hardware threads that a sandbox is pinned to, which are released when it is dropped.
#[derive(Debug)]
pub(crate) struct Pin {
	cpus: Vec<usize>,
	threads: Threads,
}

impl Shares {
	/// Create the CPU shares of a runner with the given CPU capacity in thousandths of a hardware thread.
	#[must_use]
	pub fn new(capacity: u64) -> Self {
		let state = SharesState {
			capacity: capacity.max(1),
			entries: Mutex::new(ShareEntries::default()),
		};
		Self(Arc::new(state))
	}

	/// Add a sandbox's cgroup to the shares and update every sandbox's weight.
	pub(crate) fn insert(&self, cpu: tg::sandbox::Cpu, handle: Handle) -> tg::Result<Share> {
		let mut entries = self.0.entries.lock().unwrap();
		let id = ShareId(entries.next_id);
		entries.next_id += 1;
		let entry = ShareEntry {
			cpu,
			handle,
			lender: None,
		};
		entries.entries.insert(id.0, entry);
		if let Err(error) = self.weigh(&entries, id) {
			entries.entries.remove(&id.0);
			self.weigh(&entries, id).ok();
			return Err(error);
		}
		let share = Share {
			id,
			shares: self.clone(),
		};
		Ok(share)
	}

	/// Set the sandbox whose CPU minimum a sandbox borrows from and update every sandbox's weight.
	fn set_lender(&self, id: ShareId, lender: Option<ShareId>) -> tg::Result<()> {
		let mut entries = self.0.entries.lock().unwrap();
		let Some(entry) = entries.entries.get_mut(&id.0) else {
			return Ok(());
		};
		entry.lender = lender;
		self.weigh(&entries, id)?;
		Ok(())
	}

	/// Write every sandbox's weight, failing only when the weight of the given sandbox cannot be written.
	fn weigh(&self, entries: &ShareEntries, id: ShareId) -> tg::Result<()> {
		// Get each sandbox's minimum, and the minimum that its borrowers take from it.
		let capacity = self.0.capacity;
		let minimum = |entry: &ShareEntry| entry.cpu.min.millis().min(capacity);
		let mut lent = BTreeMap::<u64, u64>::new();
		for entry in entries.entries.values() {
			if let Some(lender) = entry.lender {
				let value = lent.entry(lender.0).or_default();
				*value = value.saturating_add(minimum(entry));
			}
		}

		// Reserve each sandbox's minimum, less the minimum that its borrowers take from it.
		let bounds: Vec<_> = entries
			.entries
			.iter()
			.map(|(entry_id, entry)| {
				let minimum = minimum(entry);
				let reserved =
					minimum.saturating_sub(lent.get(entry_id).copied().unwrap_or_default());
				let max = entry
					.cpu
					.max
					.map_or(capacity, tg::sandbox::Amount::millis)
					.min(capacity);
				(
					u128::from(reserved),
					u128::from(max.saturating_sub(minimum)),
				)
			})
			.collect();

		// Write the weights.
		let weights = weights(u128::from(capacity), &bounds);
		for ((entry_id, entry), weight) in entries.entries.iter().zip(weights) {
			let result = entry
				.handle
				.write(c"cpu.weight", weight.to_string().as_bytes());
			match result {
				Err(error) if *entry_id == id.0 => return Err(error),
				Err(error) => tracing::warn!(%error, "failed to update a sandbox CPU weight"),
				Ok(()) => (),
			}
		}
		Ok(())
	}
}

impl Threads {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Pin a sandbox to the given number of the available hardware threads that the fewest sandboxes are pinned to, preferring lower threads among equally used threads.
	pub(crate) fn pin(&self, available: &[usize], width: usize) -> tg::Result<Pin> {
		if width > available.len() {
			return Err(tg::error!(
				"the CPU maximum exceeds the available hardware threads"
			));
		}
		let mut counts = self.0.lock().unwrap();
		let mut cpus = available.to_vec();
		cpus.sort_by_key(|cpu| (counts.get(cpu).copied().unwrap_or_default(), *cpu));
		cpus.truncate(width);
		cpus.sort_unstable();
		for cpu in &cpus {
			*counts.entry(*cpu).or_default() += 1;
		}
		drop(counts);
		let pin = Pin {
			cpus,
			threads: self.clone(),
		};
		Ok(pin)
	}
}

impl Share {
	#[must_use]
	pub(crate) fn id(&self) -> ShareId {
		self.id
	}

	/// Set the sandbox whose CPU minimum this sandbox borrows from.
	pub(crate) fn set_lender(&self, lender: Option<ShareId>) -> tg::Result<()> {
		self.shares.set_lender(self.id, lender)
	}
}

impl Pin {
	#[must_use]
	pub(crate) fn cpus(&self) -> &[usize] {
		&self.cpus
	}
}

impl Drop for Share {
	fn drop(&mut self) {
		let mut entries = self.shares.0.entries.lock().unwrap();
		entries.entries.remove(&self.id.0);
		if let Err(error) = self.shares.weigh(&entries, self.id) {
			tracing::warn!(%error, "failed to update the sandbox CPU weights");
		}
	}
}

impl Drop for Pin {
	fn drop(&mut self) {
		let mut counts = self.threads.0.lock().unwrap();
		for cpu in &self.cpus {
			if let Some(count) = counts.get_mut(cpu) {
				*count -= 1;
				if *count == 0 {
					counts.remove(cpu);
				}
			}
		}
	}
}

/// Get the weight of each sandbox on a runner of the given capacity from its reserved and excess CPU, giving each sandbox its reservation and dividing the remaining weight in proportion to its excess, or scaling the reservations down when they exceed the capacity.
fn weights(capacity: u128, bounds: &[(u128, u128)]) -> Vec<u128> {
	let reserved: Vec<_> = bounds
		.iter()
		.map(|(reserved, _)| (reserved * 10_000).div_ceil(capacity.max(1)).max(1))
		.collect();
	let total: u128 = reserved.iter().sum();
	if total > 10_000 {
		return reserved
			.into_iter()
			.map(|reserved| (reserved * 10_000 / total).max(1))
			.collect();
	}
	let excess: u128 = bounds.iter().map(|(_, excess)| excess).sum();
	reserved
		.into_iter()
		.zip(bounds)
		.map(|(reserved, (_, share))| reserved + (10_000 - total) * share / excess.max(1))
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn pins_choose_the_least_used_threads() {
		let threads = Threads::new();
		let available = [0, 1, 2, 3];
		let first = threads.pin(&available, 1).unwrap();
		let second = threads.pin(&available, 1).unwrap();
		let wide = threads.pin(&available, 3).unwrap();
		assert_eq!(first.cpus(), [0]);
		assert_eq!(second.cpus(), [1]);
		assert_eq!(wide.cpus(), [0, 2, 3]);
		drop(first);
		let third = threads.pin(&available, 2).unwrap();
		assert_eq!(third.cpus(), [0, 1]);
		assert!(threads.pin(&available, 5).is_err());
		drop((second, wide, third));
		assert!(threads.0.lock().unwrap().is_empty());
	}

	#[test]
	fn a_borrower_takes_its_minimum_from_its_lender() {
		use std::os::fd::IntoRawFd as _;
		let shares = Shares::new(4000);
		let temp = tangram_util::fs::Temp::new().unwrap();
		let handle = |name: &str| {
			let path = temp.path().join(name);
			std::fs::create_dir_all(&path).unwrap();
			std::fs::write(path.join("cpu.weight"), "").unwrap();
			let directory = std::fs::File::open(&path).unwrap().into_raw_fd();

			// SAFETY: The file descriptor is owned and refers to the test directory.
			unsafe { Handle::from_raw_fd(directory) }
		};
		let weight = |name: &str| {
			std::fs::read_to_string(temp.path().join(name).join("cpu.weight"))
				.unwrap()
				.trim()
				.parse::<u64>()
				.unwrap()
		};
		let cpu = tg::sandbox::Cpu::new(
			tg::sandbox::Amount::from_millis(2000),
			tg::sandbox::Amount::from_millis(2000),
		);

		// The minima of a lender and a borrower that has not yet named its lender are both reserved.
		let lender = shares.insert(cpu, handle("lender")).unwrap();
		let borrower = shares.insert(cpu, handle("borrower")).unwrap();
		assert_eq!([weight("lender"), weight("borrower")], [5000, 5000]);

		// Naming the lender moves the minimum to the borrower.
		std::fs::write(temp.path().join("lender").join("cpu.weight"), "     ").unwrap();
		borrower.set_lender(Some(lender.id())).unwrap();
		assert_eq!([weight("lender"), weight("borrower")], [1, 5000]);

		// Dropping the borrower returns the minimum to the lender.
		std::fs::write(temp.path().join("lender").join("cpu.weight"), "     ").unwrap();
		drop(borrower);
		assert_eq!(weight("lender"), 5000);
	}

	#[test]
	fn shares_reserve_each_minimum_and_divide_the_rest_by_excess() {
		use std::os::fd::IntoRawFd as _;
		let shares = Shares::new(4000);
		let temp = tangram_util::fs::Temp::new().unwrap();
		let handle = |name: &str| {
			let path = temp.path().join(name);
			std::fs::create_dir_all(&path).unwrap();
			std::fs::write(path.join("cpu.weight"), "").unwrap();
			let directory = std::fs::File::open(&path).unwrap().into_raw_fd();

			// SAFETY: The file descriptor is owned and refers to the test directory.
			unsafe { Handle::from_raw_fd(directory) }
		};
		let weight = |name: &str| {
			std::fs::read_to_string(temp.path().join(name).join("cpu.weight"))
				.unwrap()
				.parse::<u64>()
				.unwrap()
		};
		let reset = || {
			for entry in std::fs::read_dir(temp.path()).unwrap() {
				std::fs::write(entry.unwrap().path().join("cpu.weight"), "").unwrap();
			}
		};

		// A sandbox alone receives the whole weight.
		let reserved = shares
			.insert(
				tg::sandbox::Cpu::with_min(tg::sandbox::Amount::from_millis(1000)),
				handle("reserved"),
			)
			.unwrap();
		assert_eq!(weight("reserved"), 10_000);

		// Two sandboxes without minima share the weight above the reserved minimum by their excess.
		reset();
		let unlimited = shares
			.insert(tg::sandbox::Cpu::default(), handle("unlimited"))
			.unwrap();
		reset();
		let limited = shares
			.insert(
				tg::sandbox::Cpu::from(tg::sandbox::Amount::from_millis(2000)),
				handle("limited"),
			)
			.unwrap();
		assert_eq!(
			[weight("reserved"), weight("unlimited"), weight("limited")],
			[4999, 3333, 1667]
		);
		assert!(weight("reserved") * 4000 / 10_000 >= 1000 - 4);

		// Removing a sandbox updates the others.
		reset();
		drop(limited);
		assert_eq!([weight("reserved"), weight("unlimited")], [5713, 4286]);
		reset();
		drop(unlimited);
		assert_eq!(weight("reserved"), 10_000);
		drop(reserved);
	}
}
