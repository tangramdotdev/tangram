use {
	crate::cgroup::Handle,
	std::{
		collections::BTreeMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

/// The memory minimums of a runner's sandboxes, whose sum is the memory minimum of the cgroup that contains their cgroups, because a sandbox's minimum protects it only up to its parent's minimum.
#[derive(Clone, Debug)]
pub(crate) struct Minimums(Arc<MinimumsState>);

#[derive(Debug)]
struct MinimumsState {
	entries: Mutex<MinimumEntries>,
	handle: Handle,
}

#[derive(Debug, Default)]
struct MinimumEntries {
	entries: BTreeMap<u64, u64>,
	next_id: u64,
}

/// A sandbox's entry in the memory minimums, which leaves the minimums when it is dropped.
#[derive(Debug)]
pub(crate) struct Minimum {
	id: u64,
	minimums: Minimums,
}

impl Minimums {
	/// Create the memory minimums of the cgroup with the given handle.
	#[must_use]
	pub(crate) fn new(handle: Handle) -> Self {
		let state = MinimumsState {
			entries: Mutex::new(MinimumEntries::default()),
			handle,
		};
		Self(Arc::new(state))
	}

	/// Add a sandbox's memory minimum and update the sum.
	pub(crate) fn insert(&self, value: u64) -> tg::Result<Minimum> {
		let mut entries = self.0.entries.lock().unwrap();
		let id = entries.next_id;
		entries.next_id += 1;
		entries.entries.insert(id, value);
		if let Err(error) = self.write(&entries) {
			entries.entries.remove(&id);
			self.write(&entries).ok();
			return Err(error);
		}
		let minimum = Minimum {
			id,
			minimums: self.clone(),
		};
		Ok(minimum)
	}

	/// Write the sum of the memory minimums.
	fn write(&self, entries: &MinimumEntries) -> tg::Result<()> {
		let sum = entries
			.entries
			.values()
			.fold(0u64, |sum, value| sum.saturating_add(*value));
		self.0
			.handle
			.write(c"memory.min", sum.to_string().as_bytes())?;
		Ok(())
	}
}

impl Minimum {
	/// Set the sandbox's memory minimum and update the sum.
	pub(crate) fn set(&self, value: u64) -> tg::Result<()> {
		let mut entries = self.minimums.0.entries.lock().unwrap();
		let previous = entries.entries.insert(self.id, value);
		if let Err(error) = self.minimums.write(&entries) {
			if let Some(previous) = previous {
				entries.entries.insert(self.id, previous);
			}
			self.minimums.write(&entries).ok();
			return Err(error);
		}
		Ok(())
	}
}

impl Drop for Minimum {
	fn drop(&mut self) {
		let mut entries = self.minimums.0.entries.lock().unwrap();
		entries.entries.remove(&self.id);
		if let Err(error) = self.minimums.write(&entries) {
			tracing::warn!(%error, "failed to update the memory minimum of the sandbox cgroups");
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_sum_follows_each_sandbox_minimum() {
		use std::os::fd::IntoRawFd as _;
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir_all(temp.path()).unwrap();
		std::fs::write(temp.path().join("memory.min"), "").unwrap();
		let directory = std::fs::File::open(temp.path()).unwrap().into_raw_fd();

		// SAFETY: The file descriptor is owned and refers to the test directory.
		let handle = unsafe { Handle::from_raw_fd(directory) };
		let minimums = Minimums::new(handle);
		let sum = || {
			std::fs::read_to_string(temp.path().join("memory.min"))
				.unwrap()
				.trim()
				.parse::<u64>()
				.unwrap()
		};
		let reset = || std::fs::write(temp.path().join("memory.min"), "        ").unwrap();

		// Adding sandboxes adds their minimums.
		let first = minimums.insert(100).unwrap();
		assert_eq!(sum(), 100);
		reset();
		let second = minimums.insert(200).unwrap();
		assert_eq!(sum(), 300);
		reset();
		let unlimited = minimums.insert(0).unwrap();
		assert_eq!(sum(), 300);

		// Setting a sandbox's minimum replaces it in the sum.
		reset();
		second.set(50).unwrap();
		assert_eq!(sum(), 150);
		reset();
		unlimited.set(25).unwrap();
		assert_eq!(sum(), 175);

		// Dropping a sandbox subtracts its minimum.
		reset();
		drop(first);
		assert_eq!(sum(), 75);
		reset();
		drop(second);
		assert_eq!(sum(), 25);
		reset();
		drop(unlimited);
		assert_eq!(sum(), 0);
	}

	#[test]
	fn a_failed_write_leaves_the_sum_unchanged() {
		use std::os::fd::IntoRawFd as _;
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir_all(temp.path()).unwrap();
		let directory = std::fs::File::open(temp.path()).unwrap().into_raw_fd();

		// SAFETY: The file descriptor is owned and refers to the test directory.
		let handle = unsafe { Handle::from_raw_fd(directory) };
		let minimums = Minimums::new(handle);

		// An insert fails without the memory minimum file and leaves no entry behind.
		assert!(minimums.insert(100).is_err());
		assert!(minimums.0.entries.lock().unwrap().entries.is_empty());

		// A set that fails restores the previous minimum.
		std::fs::write(temp.path().join("memory.min"), "").unwrap();
		let minimum = minimums.insert(100).unwrap();
		std::fs::remove_file(temp.path().join("memory.min")).unwrap();
		assert!(minimum.set(200).is_err());
		assert_eq!(
			minimums
				.0
				.entries
				.lock()
				.unwrap()
				.entries
				.values()
				.sum::<u64>(),
			100
		);
	}
}
