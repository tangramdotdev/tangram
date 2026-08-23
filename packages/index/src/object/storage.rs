use tangram_util::serde::is_false;

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Storage {
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub subtree: bool,
}

impl Storage {
	pub fn merge(&mut self, other: &Self) {
		self.subtree = self.subtree || other.subtree;
	}
}
