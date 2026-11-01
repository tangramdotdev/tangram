mod id;

pub use self::{data::Data, id::Id};

pub mod control;
pub mod create;
pub mod data;
pub mod delete;
pub mod get;
pub mod list;
pub mod token;

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(default)]
pub struct Capacity {
	#[tangram_serialize(id = 0)]
	pub cpu: crate::sandbox::Cpu,

	#[tangram_serialize(id = 1)]
	pub memory: crate::sandbox::Resource<u64>,
	#[tangram_serialize(id = 4)]
	pub sandboxes: u64,
	#[tangram_serialize(id = 2)]
	pub storage: crate::sandbox::Resource<u64>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(default, deny_unknown_fields)]
pub struct Overcommit {
	#[tangram_serialize(id = 0)]
	pub cpu: u64,

	#[tangram_serialize(id = 1)]
	pub memory: u64,

	#[tangram_serialize(id = 2)]
	pub storage: u64,
}

impl Capacity {
	#[must_use]
	pub fn contains(self, requested: Self, overcommit: Overcommit) -> bool {
		let cpu = cpu_millis(self.cpu);
		let requested_cpu = cpu_millis(requested.cpu);
		cpu.contains(requested_cpu, overcommit.cpu)
			&& self.memory.contains(requested.memory, overcommit.memory)
			&& requested.sandboxes <= self.sandboxes
			&& self.storage.contains(requested.storage, overcommit.storage)
	}

	/// Borrow the requested capacity from a waiting lender's capacity, returning the borrowed capacity and the capacity that the borrower must also acquire from the runner. The lender lends all of its CPU, including its minimum, because a waiting lender does not use it. The lender keeps its memory and storage minima, because a waiting lender still holds its data, and lends only the range above them. The borrower acquires from the runner whatever it requests beyond what the lender lends, and a requested resource without a maximum takes whatever the lender lends.
	#[must_use]
	pub fn try_borrow(self, requested: Self) -> Option<(Self, Self)> {
		// Borrow the sandbox.
		if requested.sandboxes > self.sandboxes {
			return None;
		}

		// Borrow the CPU, including the lender's minimum.
		let (cpu, acquired_cpu) = borrow(cpu_millis(requested.cpu), cpu_millis(self.cpu));

		// Borrow the memory and storage above the lender's minima.
		let lent = |resource: crate::sandbox::Resource<u64>| crate::sandbox::Resource {
			max: resource.max.map(|max| max.saturating_sub(resource.min)),
			min: 0,
		};
		let (memory, acquired_memory) = borrow(requested.memory, lent(self.memory));
		let (storage, acquired_storage) = borrow(requested.storage, lent(self.storage));

		// Create the borrowed and acquired capacities.
		let borrowed = Self {
			cpu: cpu_amounts(cpu),
			memory,
			sandboxes: requested.sandboxes,
			storage,
		};
		let acquired = Self {
			cpu: cpu_amounts(acquired_cpu),
			memory: acquired_memory,
			sandboxes: 0,
			storage: acquired_storage,
		};

		Some((borrowed, acquired))
	}

	#[must_use]
	pub fn subtract(self, requested: Self, overcommit: Overcommit) -> Self {
		let cpu = cpu_millis(self.cpu);
		let requested_cpu = cpu_millis(requested.cpu);
		let remaining = cpu.subtract(requested_cpu, overcommit.cpu);
		let max = remaining.max.map(crate::sandbox::Amount::from_millis);
		let min = crate::sandbox::Amount::from_millis(remaining.min);
		let cpu = crate::sandbox::Cpu { max, min };
		let memory = self.memory.subtract(requested.memory, overcommit.memory);
		let sandboxes = self.sandboxes.saturating_sub(requested.sandboxes);
		let storage = self.storage.subtract(requested.storage, overcommit.storage);
		Self {
			cpu,
			memory,
			sandboxes,
			storage,
		}
	}
}

impl Default for Overcommit {
	fn default() -> Self {
		Self {
			cpu: 4,
			memory: 1,
			storage: 1,
		}
	}
}

/// Borrow a requested resource from what a lender lends, returning the borrowed resource, whose maximum is the lent maximum when it requests none, and the resource that the borrower must acquire beyond what is lent.
fn borrow(
	requested: crate::sandbox::Resource<u64>,
	lent: crate::sandbox::Resource<u64>,
) -> (crate::sandbox::Resource<u64>, crate::sandbox::Resource<u64>) {
	// Borrow as much of the requested minimum as is lent, and the range of the lent maximum above it.
	let minimum = requested.min.min(lent.min);
	let range = lent.max.map(|max| max.saturating_sub(minimum));

	// Acquire the rest of the requested minimum, and the requested range beyond the lent range.
	let acquired_min = requested.min - minimum;
	let acquired_range = match (requested.max, range) {
		(Some(max), Some(range)) => max.saturating_sub(requested.min).saturating_sub(range),
		_ => 0,
	};
	let acquired = crate::sandbox::Resource {
		max: (acquired_range > 0).then(|| acquired_min.saturating_add(acquired_range)),
		min: acquired_min,
	};

	// Give a borrower without a maximum the lent range above its minimum.
	let max = requested
		.max
		.or_else(|| range.map(|range| requested.min.saturating_add(range)));
	let borrowed = crate::sandbox::Resource {
		max,
		min: requested.min,
	};

	(borrowed, acquired)
}

fn cpu_amounts(cpu: crate::sandbox::Resource<u64>) -> crate::sandbox::Cpu {
	crate::sandbox::Cpu {
		max: cpu.max.map(crate::sandbox::Amount::from_millis),
		min: crate::sandbox::Amount::from_millis(cpu.min),
	}
}

fn cpu_millis(cpu: crate::sandbox::Cpu) -> crate::sandbox::Resource<u64> {
	crate::sandbox::Resource {
		max: cpu.max.map(crate::sandbox::Amount::millis),
		min: cpu.min.millis(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn overcommit_consumes_only_capacity_left_after_minima() {
		let capacity = Capacity {
			cpu: crate::sandbox::Cpu::new(
				crate::sandbox::Amount::from_millis(2000),
				crate::sandbox::Amount::from_millis(8000),
			),
			memory: crate::sandbox::Resource::new(100, 200),
			sandboxes: 2,
			storage: crate::sandbox::Resource::new(100, 200),
		};
		let requested = Capacity {
			cpu: crate::sandbox::Cpu::new(
				crate::sandbox::Amount::from_millis(250),
				crate::sandbox::Amount::from_millis(1000),
			),
			memory: crate::sandbox::Resource::new(80, 100),
			sandboxes: 1,
			storage: crate::sandbox::Resource::new(80, 100),
		};
		let overcommit = Overcommit {
			cpu: 4,
			memory: 2,
			storage: 2,
		};
		assert!(capacity.contains(requested, overcommit));
		let available = capacity.subtract(requested, overcommit);
		assert_eq!(available.cpu.max.unwrap().millis(), 6250);
		assert_eq!(available.sandboxes, 1);
		assert_eq!(available.cpu.min.millis(), 1562);
		for resource in [available.memory, available.storage] {
			assert_eq!(resource, crate::sandbox::Resource::new(10, 20));
			assert!(resource.contains(20.into(), 2));
			assert!(!resource.contains(crate::sandbox::Resource::new(11, 11), 2));
		}
	}

	#[test]
	fn a_request_without_a_maximum_consumes_only_its_minimum() {
		let capacity = crate::sandbox::Resource::new(100, 400);
		let requested = crate::sandbox::Resource::with_min(50);
		assert!(capacity.contains(requested, 4));
		assert_eq!(
			capacity.subtract(requested, 4),
			crate::sandbox::Resource::new(50, 200)
		);
		assert!(capacity.contains(crate::sandbox::Resource::default(), 4));
		assert_eq!(
			capacity.subtract(crate::sandbox::Resource::default(), 4),
			capacity
		);
	}

	#[test]
	fn sandboxes_bound_admission_and_borrowing() {
		let capacity = Capacity {
			sandboxes: 1,
			..Default::default()
		};
		let requested = Capacity {
			sandboxes: 1,
			..Default::default()
		};
		let overcommit = Overcommit::default();
		assert!(capacity.contains(requested, overcommit));
		let available = capacity.subtract(requested, overcommit);
		assert!(!available.contains(requested, overcommit));
		assert!(capacity.try_borrow(requested).is_some());
		assert!(available.try_borrow(requested).is_none());
	}

	#[test]
	fn a_borrower_without_a_maximum_takes_the_lender_maximum() {
		let lender = Capacity {
			cpu: crate::sandbox::Amount::from_millis(1000).into(),
			memory: crate::sandbox::Resource::new(0, 100),
			sandboxes: 2,
			..Default::default()
		};
		let requested = Capacity {
			sandboxes: 1,
			..Default::default()
		};
		let (borrowed, acquired) = lender.try_borrow(requested).unwrap();
		assert_eq!(
			borrowed.cpu.max,
			Some(crate::sandbox::Amount::from_millis(1000))
		);
		assert_eq!(borrowed.memory.max, Some(100));
		assert_eq!(borrowed.storage.max, None);
		assert_eq!(borrowed.sandboxes, 1);
		assert_eq!(acquired, Capacity::default());
	}

	#[test]
	fn a_borrower_borrows_what_the_lender_lends_and_acquires_the_rest() {
		let lender = Capacity {
			cpu: crate::sandbox::Cpu::new(
				crate::sandbox::Amount::from_millis(2000),
				crate::sandbox::Amount::from_millis(4000),
			),
			memory: crate::sandbox::Resource::new(60, 100),
			sandboxes: 1,
			..Default::default()
		};

		// A borrower takes up to the lender's CPU minimum and acquires its own memory minimum.
		let requested = Capacity {
			cpu: crate::sandbox::Cpu::with_min(crate::sandbox::Amount::from_millis(1000)),
			memory: crate::sandbox::Resource::with_min(50),
			sandboxes: 1,
			..Default::default()
		};
		let (borrowed, acquired) = lender.try_borrow(requested).unwrap();
		assert_eq!(
			borrowed.cpu,
			crate::sandbox::Cpu::new(
				crate::sandbox::Amount::from_millis(1000),
				crate::sandbox::Amount::from_millis(4000),
			)
		);
		assert_eq!(borrowed.memory, crate::sandbox::Resource::new(50, 90));
		assert_eq!(acquired.memory, crate::sandbox::Resource::with_min(50));
		assert_eq!(acquired.cpu, crate::sandbox::Cpu::default());

		// A borrower acquires the CPU minimum beyond the lender's from the runner.
		let requested = Capacity {
			cpu: crate::sandbox::Cpu::with_min(crate::sandbox::Amount::from_millis(3000)),
			sandboxes: 1,
			..Default::default()
		};
		let (borrowed, acquired) = lender.try_borrow(requested).unwrap();
		assert_eq!(borrowed.cpu.min.millis(), 3000);
		assert_eq!(
			acquired.cpu,
			crate::sandbox::Cpu::with_min(crate::sandbox::Amount::from_millis(1000))
		);

		// A borrower acquires the range beyond what the lender lends from the runner.
		let requested = Capacity {
			cpu: crate::sandbox::Cpu::from(crate::sandbox::Amount::from_millis(6000)),
			memory: crate::sandbox::Resource::new(0, 50),
			sandboxes: 1,
			..Default::default()
		};
		let (borrowed, acquired) = lender.try_borrow(requested).unwrap();
		assert_eq!(borrowed.memory, crate::sandbox::Resource::new(0, 50));
		assert_eq!(acquired.memory, crate::sandbox::Resource::new(0, 10));
		assert_eq!(
			acquired.cpu,
			crate::sandbox::Cpu::new(
				crate::sandbox::Amount::from_millis(0),
				crate::sandbox::Amount::from_millis(2000),
			)
		);

		// A borrower cannot borrow more sandboxes than the lender has.
		let requested = Capacity {
			sandboxes: 2,
			..Default::default()
		};
		assert!(lender.try_borrow(requested).is_none());
	}
}
