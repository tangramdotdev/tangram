use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub account: Option<crate::usage::Account>,
	/// The runner attempt, or `None` to preserve the indexed attempt.
	#[tangram_serialize(id = 7)]
	pub attempt: Option<String>,
	#[tangram_serialize(id = 1)]
	pub created_at: i64,
	#[tangram_serialize(id = 2)]
	pub data: Option<tg::sandbox::get::Output>,
	#[tangram_serialize(id = 3)]
	pub id: tg::sandbox::Id,
	/// The sandbox location, or `None` to preserve the indexed location.
	#[tangram_serialize(id = 6)]
	pub location: Option<tg::Location>,
	#[tangram_serialize(id = 4)]
	pub runner: Option<tg::runner::Id>,
	#[tangram_serialize(id = 5)]
	pub touched_at: i64,
}
