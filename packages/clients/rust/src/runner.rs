mod id;

pub use self::{data::Data, id::Id};

pub mod control;
pub mod create;
pub mod data;
pub mod delete;
pub mod list;
pub mod token;

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
