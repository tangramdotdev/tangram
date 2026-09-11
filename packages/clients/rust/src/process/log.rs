#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct End {
	#[tangram_serialize(id = 0)]
	pub position: u64,
	#[tangram_serialize(id = 1)]
	pub stderr_position: u64,
	#[tangram_serialize(id = 2)]
	pub stdout_position: u64,
}
