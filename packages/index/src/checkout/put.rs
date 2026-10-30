use {tangram_client::prelude::*, tangram_util::serde::is_default};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub dependencies: Vec<tg::Id>,

	#[tangram_serialize(id = 1)]
	pub id: tg::Id,

	/// Whether to delete the checkout's existing dependencies before putting the new ones.
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_default")]
	pub replace: bool,

	#[tangram_serialize(id = 2)]
	pub touched_at: i64,
}
