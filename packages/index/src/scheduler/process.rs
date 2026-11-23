use tangram_client::prelude::*;

#[cfg(test)]
mod tests;

pub mod delete;
pub mod put;

/// The change to the entry that a put or delete makes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Change {
	/// Leave the entry as it is.
	Keep,

	/// Store the entry.
	Store(SchedulerProcess),
}

/// The entry of a process in a scheduler's queue, which holds the process's spawn request while the scheduler holds the process, or records that the scheduler stopped holding the process, so that the puts and the deletes of the entry resolve in any order and any number of times. The index removes the record of a delete by its age.
#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub enum SchedulerProcess {
	/// The scheduler stopped holding the process at the time of the first delete of the entry to apply.
	#[tangram_serialize(id = 0)]
	Deleted(i64),

	/// The scheduler holds the process with the spawn request.
	#[tangram_serialize(id = 1)]
	Queued(Vec<u8>),
}

/// A process in a scheduler's queue.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueuedProcess {
	pub id: tg::process::Id,
	pub request: Vec<u8>,
}

impl SchedulerProcess {
	/// Resolve a put of a process's spawn request, which does not apply after a delete of the entry.
	#[must_use]
	pub fn put(existing: Option<&Self>, request: &[u8]) -> Change {
		if let Some(Self::Deleted(_)) = existing {
			return Change::Keep;
		}
		Change::Store(Self::Queued(request.to_vec()))
	}

	/// Resolve a delete of the entry, which records the delete unless an earlier delete applied, so that no put applies afterward.
	#[must_use]
	pub fn delete(existing: Option<&Self>, deleted_at: i64) -> Change {
		if let Some(Self::Deleted(_)) = existing {
			return Change::Keep;
		}
		Change::Store(Self::Deleted(deleted_at))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		let entry = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the scheduler process"))?;
		Ok(entry)
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		let bytes = tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the scheduler process"))?;
		Ok(bytes)
	}
}
