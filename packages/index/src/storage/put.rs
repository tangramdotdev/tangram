use {super::Owner, tangram_client::prelude::*};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct ObjectArg {
	#[tangram_serialize(id = 0)]
	pub object: tg::object::Id,

	#[tangram_serialize(id = 1)]
	pub owner: Owner,

	#[tangram_serialize(id = 2)]
	pub touched_at: i64,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct ProcessArg {
	#[tangram_serialize(id = 0)]
	pub owner: Owner,

	#[tangram_serialize(id = 1)]
	pub process: tg::process::Id,

	#[tangram_serialize(id = 2)]
	pub touched_at: i64,
}
