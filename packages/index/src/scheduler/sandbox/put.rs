use tangram_client::prelude::*;

/// A put of the request of a boot that a scheduler accepted, which the scheduler recovers from the index if it restarts before the sandbox's runner connects.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub request: Vec<u8>,

	#[tangram_serialize(id = 2)]
	pub sandbox: tg::sandbox::Id,

	#[tangram_serialize(id = 3)]
	pub scheduler: tg::scheduler::Id,
}
