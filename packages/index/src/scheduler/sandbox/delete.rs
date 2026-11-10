use tangram_client::prelude::*;

/// A delete of the request of a boot that a scheduler no longer owns, because the boot was dequeued, ended unplaced, or its runner connected.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,
}
