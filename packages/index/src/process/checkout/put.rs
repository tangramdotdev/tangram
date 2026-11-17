use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	/// The checkouts that the process retains until it finishes.
	#[tangram_serialize(id = 0)]
	pub checkouts: Vec<tg::Id>,

	#[tangram_serialize(id = 1)]
	pub process: tg::process::Id,
}
