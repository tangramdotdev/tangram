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
	pub memory: u64,
}

impl Capacity {
	/// Test a combined request against convertible cores and existing shared slots.
	#[must_use]
	pub fn contains(self, requested: Self, oversubscription: u64) -> bool {
		if oversubscription == 0
			|| self.cpu.dedicated < requested.cpu.dedicated
			|| self.memory < requested.memory
		{
			return false;
		}
		let slots = u128::from(self.cpu.shared)
			+ u128::from(self.cpu.dedicated - requested.cpu.dedicated)
				* u128::from(oversubscription);
		u128::from(requested.cpu.shared) <= slots
	}

	/// Inherit an exclusive reservation, converting only the cores needed by the request.
	#[must_use]
	pub fn try_borrow(self, requested: Self, oversubscription: u64) -> Option<Self> {
		if !self.contains(requested, oversubscription) {
			return None;
		}
		let remaining = self.subtract(requested, oversubscription);
		let dedicated = remaining
			.cpu
			.dedicated
			.checked_add(requested.cpu.dedicated)?;
		let shared = remaining.cpu.shared.checked_add(requested.cpu.shared)?;
		let cpu = crate::sandbox::Cpu { dedicated, shared };
		Some(Self {
			cpu,
			memory: self.memory,
		})
	}

	#[must_use]
	pub fn subtract(self, requested: Self, oversubscription: u64) -> Self {
		let dedicated = self.cpu.dedicated.saturating_sub(requested.cpu.dedicated);
		let oversubscription = oversubscription.max(1);
		let missing = requested.cpu.shared.saturating_sub(self.cpu.shared);
		let converted = missing.div_ceil(oversubscription).min(dedicated);
		let shared =
			u128::from(self.cpu.shared) + u128::from(converted) * u128::from(oversubscription);
		let shared = u64::try_from(shared.saturating_sub(u128::from(requested.cpu.shared)))
			.unwrap_or(u64::MAX);
		let dedicated = dedicated.saturating_sub(converted);
		let cpu = crate::sandbox::Cpu { dedicated, shared };
		let memory = self.memory.saturating_sub(requested.memory);
		Self { cpu, memory }
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn shared_requests_convert_cores_without_inventing_slots() {
		let cpu = crate::sandbox::Cpu {
			dedicated: 4,
			shared: 0,
		};
		let capacity = Capacity { cpu, memory: 16 };
		let requested = Capacity {
			cpu: 1.into(),
			memory: 1,
		};
		assert!(capacity.contains(requested, 4));
		let remaining = capacity.subtract(requested, 4);
		assert_eq!(
			remaining.cpu,
			crate::sandbox::Cpu {
				dedicated: 3,
				shared: 3
			}
		);
		assert_eq!(remaining.memory, 15);
		assert_eq!(
			Capacity::default().subtract(requested, 4),
			Capacity::default()
		);
	}

	#[test]
	fn borrowing_preserves_headroom_and_only_converts_required_cores() {
		let cpu = crate::sandbox::Cpu {
			dedicated: 4,
			shared: 1,
		};
		let capacity = Capacity { cpu, memory: 16 };
		let requested = Capacity {
			cpu: crate::sandbox::Cpu {
				dedicated: 1,
				shared: 2,
			},
			memory: 1,
		};
		let inherited = capacity.try_borrow(requested, 4).unwrap();
		assert_eq!(inherited.memory, 16);
		assert_eq!(
			inherited.cpu,
			crate::sandbox::Cpu {
				dedicated: 3,
				shared: 5
			}
		);
		assert_eq!(inherited.try_borrow(requested, 4), Some(inherited));
		assert!(inherited.try_borrow(capacity, 4).is_none());
	}

	#[test]
	fn mixed_requests_compete_for_convertible_cores() {
		let cpu = crate::sandbox::Cpu {
			dedicated: 2,
			shared: 1,
		};
		let capacity = Capacity { cpu, memory: 16 };
		let cpu = crate::sandbox::Cpu {
			dedicated: 1,
			shared: 5,
		};
		let requested = Capacity { cpu, memory: 1 };
		assert!(capacity.contains(requested, 4));
		assert_eq!(
			capacity.subtract(requested, 4).cpu,
			crate::sandbox::Cpu::default()
		);
		let requested = Capacity {
			cpu: crate::sandbox::Cpu {
				dedicated: 2,
				shared: 2,
			},
			memory: 1,
		};
		assert!(!capacity.contains(requested, 4));
	}
}
