use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub id: tg::group::Id,

	#[tangram_serialize(id = 1)]
	pub parent: Option<tg::Id>,

	/// The permissions to put after the group, so that the index finds the group and its ancestors when it puts their visibility entries.
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Vec::is_empty")]
	pub permissions: Vec<crate::permission::put::Arg>,

	#[tangram_serialize(id = 2)]
	pub specifier: tg::Specifier,
}
