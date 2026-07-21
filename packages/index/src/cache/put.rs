use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub dependencies: Vec<tg::artifact::Id>,
	#[tangram_serialize(id = 1)]
	pub id: tg::artifact::Id,
	#[tangram_serialize(id = 2)]
	pub touched_at: i64,
}
