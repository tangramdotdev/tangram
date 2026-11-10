use tangram_client::prelude::*;

pub mod delete;
pub mod put;

#[cfg(test)]
mod tests;

/// The scheduler that owns a boot of a sandbox, or the latest boot that a scheduler stopped owning, so that the puts and deletes of the boots resolve by boot in any order.
#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct SandboxScheduler {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<tg::scheduler::Id>,
}

/// The change to the keys that a put or delete makes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Change {
	/// The scheduler whose sandbox key is removed.
	pub removed: Option<tg::scheduler::Id>,

	/// The record that replaces the existing one.
	pub stored: SandboxScheduler,
}

impl SandboxScheduler {
	/// Resolve a put of a boot by a scheduler, returning `None` if a later boot, or a delete of the same boot, already applied.
	#[must_use]
	pub fn put(
		existing: Option<&Self>,
		boot: u64,
		scheduler: &tg::scheduler::Id,
	) -> Option<Change> {
		if let Some(existing) = existing
			&& (existing.boot > boot || (existing.boot == boot && existing.scheduler.is_none()))
		{
			return None;
		}
		let removed = existing
			.and_then(|existing| existing.scheduler.clone())
			.filter(|existing| existing != scheduler);
		let stored = Self {
			boot,
			scheduler: Some(scheduler.clone()),
		};
		let change = Change { removed, stored };

		Some(change)
	}

	/// Resolve a delete of a boot, returning `None` if a later boot already applied, and otherwise recording the boot as deleted, so that its put does not apply afterward.
	#[must_use]
	pub fn delete(existing: Option<&Self>, boot: u64) -> Option<Change> {
		if existing.is_some_and(|existing| existing.boot > boot) {
			return None;
		}
		let removed = existing.and_then(|existing| existing.scheduler.clone());
		let stored = Self {
			boot,
			scheduler: None,
		};
		let change = Change { removed, stored };

		Some(change)
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the sandbox scheduler"))
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the sandbox scheduler"))
	}
}
