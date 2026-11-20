use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub account: Option<crate::usage::Account>,

	/// The captures to put after the tag, which check the tag's version.
	#[tangram_serialize(
		default,
		id = 9,
		skip_serializing_if = "crate::permission::capture::Captures::is_empty"
	)]
	pub captures: crate::permission::capture::Captures,

	/// The groups to put before the tag, so that the index finds the tag's ancestors when it puts their visibility entries.
	#[tangram_serialize(default, id = 11, skip_serializing_if = "Vec::is_empty")]
	pub groups: Vec<crate::group::put::Arg>,

	#[tangram_serialize(id = 0)]
	pub id: tg::tag::Id,

	#[tangram_serialize(id = 2)]
	pub name: String,

	#[tangram_serialize(id = 3)]
	pub parent: Option<tg::Id>,

	/// The permissions to put after the groups and the tag, so that the index finds the tag's ancestors when it puts their visibility entries.
	#[tangram_serialize(default, id = 10, skip_serializing_if = "Vec::is_empty")]
	pub permissions: Vec<crate::permission::put::Arg>,

	#[tangram_serialize(id = 5)]
	pub specifier: tg::Specifier,

	#[tangram_serialize(id = 1)]
	pub target: tg::Either<tg::object::Id, tg::process::Id>,

	#[tangram_serialize(id = 8)]
	pub touched_at: i64,

	#[tangram_serialize(id = 7)]
	pub version: String,
}
