use tangram_client::prelude::*;

/// A put of the spawn request of a process on its way into an existing sandbox, which a scheduler writes with the process's queued record and recovers from the index if it restarts before the sandbox's runner accepts the process.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub process: tg::process::Id,

	#[tangram_serialize(id = 1)]
	pub request: Vec<u8>,

	#[tangram_serialize(id = 2)]
	pub sandbox: tg::sandbox::Id,

	#[tangram_serialize(id = 3)]
	pub scheduler: tg::scheduler::Id,
}
