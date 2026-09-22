use crate::prelude::*;

#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Storage {
	#[tangram_serialize(id = 0)]
	Object(tg::object::Storage),

	#[tangram_serialize(id = 1)]
	Process(tg::process::Storage),
}
