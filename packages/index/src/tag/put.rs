use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub id: tg::tag::Id,
	#[tangram_serialize(id = 1)]
	pub target: tg::Either<tg::object::Id, tg::process::Id>,
	#[tangram_serialize(id = 2)]
	pub name: String,
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub owner: Option<crate::storage::Owner>,
	#[tangram_serialize(id = 3)]
	pub parent: Option<tg::Id>,
	#[tangram_serialize(id = 4)]
	pub permissions: Vec<tg::grant::Permission>,
	#[tangram_serialize(id = 5)]
	pub specifier: tg::Specifier,
}
