mod id;

pub use self::id::Id;

pub mod control;

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Capacity {
	#[tangram_serialize(id = 0)]
	pub cpus: u64,

	#[tangram_serialize(id = 1)]
	pub memory: u64,
}
