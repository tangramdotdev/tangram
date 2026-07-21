use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub id: tg::group::Id,
	#[tangram_serialize(id = 1)]
	pub parent: Option<tg::Id>,
	#[tangram_serialize(id = 2)]
	pub specifier: tg::Specifier,
}
