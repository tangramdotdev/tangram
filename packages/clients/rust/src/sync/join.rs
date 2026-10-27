use crate::prelude::*;

/// The sync that a sync joins.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Join {
	/// The command sync of the attach that a nonce names.
	#[tangram_serialize(id = 0)]
	Attach(tg::process::Nonce),

	/// One of the syncs of a process, which its control service resolves.
	#[tangram_serialize(id = 1)]
	Process(Process),

	/// A sync named by its referent.
	#[tangram_serialize(id = 2)]
	Sync(tg::Referent<tg::sync::Id>),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Process {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[tangram_serialize(id = 1)]
	pub kind: Kind,
}

/// The kind of a process's sync.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
	/// The sync that pushes the process's command objects.
	#[tangram_serialize(id = 0)]
	Command,

	/// The sync that pushes the process's outcome objects.
	#[tangram_serialize(id = 1)]
	Outcome,
}

impl Join {
	/// Get the sync's referent, if the join names one.
	#[must_use]
	pub fn sync(&self) -> Option<&tg::Referent<tg::sync::Id>> {
		match self {
			Self::Attach(_) | Self::Process(_) => None,
			Self::Sync(sync) => Some(sync),
		}
	}
}
