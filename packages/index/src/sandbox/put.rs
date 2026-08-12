use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub account: Option<crate::usage::Account>,
	#[tangram_serialize(id = 1)]
	pub created_at: i64,
	#[tangram_serialize(id = 2)]
	pub data: Option<tg::sandbox::get::Output>,
	#[tangram_serialize(id = 3)]
	pub id: tg::sandbox::Id,
	#[tangram_serialize(id = 4)]
	pub runner: Option<tg::runner::Id>,
	#[tangram_serialize(id = 5)]
	pub touched_at: i64,
}
