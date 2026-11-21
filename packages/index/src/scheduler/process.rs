use tangram_client::prelude::*;

pub mod delete;
pub mod put;

#[cfg(test)]
mod tests;

/// The entry of a process in a scheduler's queue, which holds the process's spawn request while the scheduler holds the process, or records that the scheduler handed the process off before the put of the entry applied, so that the put and the delete of the entry resolve in any order and leave no record once both apply.
#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct SchedulerProcess {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub request: Option<Vec<u8>>,
}

/// A process in a scheduler's queue.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueuedProcess {
	pub id: tg::process::Id,
	pub request: Vec<u8>,
}

/// The change to the entry that a put or delete makes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Change {
	/// Remove the entry.
	Clear,

	/// Leave the entry as it is.
	Keep,

	/// Store the entry.
	Store(SchedulerProcess),
}

impl SchedulerProcess {
	/// Resolve a put of a process's spawn request, which does not apply after the delete of the entry, and which removes the delete's record, so that no record of the entry remains once both apply.
	#[must_use]
	pub fn put(existing: Option<&Self>, request: &[u8]) -> Change {
		if existing.is_some_and(|existing| existing.request.is_none()) {
			return Change::Clear;
		}
		let stored = Self {
			request: Some(request.to_vec()),
		};
		Change::Store(stored)
	}

	/// Resolve a delete of the entry, which removes the entry if its put applied, and otherwise records the delete, so that the put does not apply afterward.
	#[must_use]
	pub fn delete(existing: Option<&Self>) -> Change {
		match existing {
			Some(Self { request: Some(_) }) => Change::Clear,
			Some(Self { request: None }) => Change::Keep,
			None => Change::Store(Self { request: None }),
		}
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the scheduler process"))
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the scheduler process"))
	}
}
