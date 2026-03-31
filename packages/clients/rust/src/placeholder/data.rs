#[derive(
	Clone,
	Debug,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Placeholder {
	#[tangram_serialize(id = 0)]
	pub name: String,
}
