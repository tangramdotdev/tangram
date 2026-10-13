mod id;

pub use self::{data::Data, id::Id};

pub mod control;
pub mod create;
pub mod data;
pub mod delete;
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
pub struct Capacity {
	#[tangram_serialize(id = 0)]
	pub cpu: crate::sandbox::Cpu,

	#[tangram_serialize(id = 1)]
	pub memory: crate::sandbox::Resource<u64>,
	#[tangram_serialize(id = 2)]
	pub storage: crate::sandbox::Resource<u64>,
	#[tangram_serialize(id = 3)]
	pub swap: crate::sandbox::Resource<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Overcommit {
	pub cpu: u64,
	pub memory: u64,
	pub storage: u64,
	pub swap: u64,
}

impl Capacity {
	#[must_use]
	pub fn contains(self, requested: Self, overcommit: Overcommit) -> bool {
		let cpu = crate::sandbox::Resource::new(self.cpu.min.millis(), self.cpu.max.millis());
		let requested_cpu =
			crate::sandbox::Resource::new(requested.cpu.min.millis(), requested.cpu.max.millis());
		cpu.contains(requested_cpu, overcommit.cpu)
			&& self.memory.contains(requested.memory, overcommit.memory)
			&& self.storage.contains(requested.storage, overcommit.storage)
			&& self.swap.contains(requested.swap, overcommit.swap)
	}

	#[must_use]
	pub fn try_borrow(self, requested: Self, _overcommit: Overcommit) -> Option<Self> {
		(self.cpu.min.millis() == 0
			&& self.memory.min == 0
			&& self.storage.min == 0
			&& self.swap.min == 0
			&& self.cpu.min >= requested.cpu.min
			&& self.cpu.max >= requested.cpu.max
			&& self.memory.min >= requested.memory.min
			&& self.memory.max >= requested.memory.max
			&& self.storage.min >= requested.storage.min
			&& self.storage.max >= requested.storage.max
			&& self.swap.min >= requested.swap.min
			&& self.swap.max >= requested.swap.max)
			.then_some(self)
	}

	#[must_use]
	pub fn subtract(self, requested: Self, overcommit: Overcommit) -> Self {
		let cpu = crate::sandbox::Resource::new(self.cpu.min.millis(), self.cpu.max.millis());
		let requested_cpu =
			crate::sandbox::Resource::new(requested.cpu.min.millis(), requested.cpu.max.millis());
		let remaining = cpu.subtract(requested_cpu, overcommit.cpu);
		let max = crate::sandbox::Amount::from_millis(remaining.max);
		let min = crate::sandbox::Amount::from_millis(remaining.min);
		let cpu = crate::sandbox::Cpu { max, min };
		let memory = self.memory.subtract(requested.memory, overcommit.memory);
		let storage = self.storage.subtract(requested.storage, overcommit.storage);
		let swap = self.swap.subtract(requested.swap, overcommit.swap);
		Self {
			cpu,
			memory,
			storage,
			swap,
		}
	}
}

impl Default for Overcommit {
	fn default() -> Self {
		Self {
			cpu: 4,
			memory: 1,
			storage: 1,
			swap: 1,
		}
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
			storage: crate::sandbox::Resource::new(100, 200),
			swap: crate::sandbox::Resource::new(100, 200),
		};
		let requested = Capacity {
			cpu: crate::sandbox::Cpu::new(
				crate::sandbox::Amount::from_millis(250),
				crate::sandbox::Amount::from_millis(1000),
			),
			memory: crate::sandbox::Resource::new(80, 100),
			storage: crate::sandbox::Resource::new(80, 100),
			swap: crate::sandbox::Resource::new(80, 100),
		};
		let overcommit = Overcommit {
			cpu: 4,
			memory: 2,
			storage: 2,
			swap: 2,
		};
		assert!(capacity.contains(requested, overcommit));
		let available = capacity.subtract(requested, overcommit);
		assert_eq!(available.cpu.max.millis(), 6250);
		assert_eq!(available.cpu.min.millis(), 1562);
		for resource in [available.memory, available.storage, available.swap] {
			assert_eq!(resource, crate::sandbox::Resource::new(10, 20));
			assert!(resource.contains(20.into(), 2));
			assert!(!resource.contains(crate::sandbox::Resource::new(11, 11), 2));
		}
	}
}
