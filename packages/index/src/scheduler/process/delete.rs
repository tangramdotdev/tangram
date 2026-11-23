use tangram_client::prelude::*;

/// A delete of the spawn request of a process that a scheduler no longer holds, because the sandbox's runner accepted it or the scheduler finished it.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	/// The time of the delete, from which the index removes the record of the delete by its age.
	#[tangram_serialize(id = 3)]
	pub deleted_at: i64,

	#[tangram_serialize(id = 0)]
	pub process: tg::process::Id,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,

	#[tangram_serialize(id = 2)]
	pub scheduler: tg::scheduler::Id,
}
