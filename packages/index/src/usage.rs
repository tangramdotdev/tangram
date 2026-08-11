pub use tangram_client::usage::Account;

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	num_derive::FromPrimitive,
	num_derive::ToPrimitive,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[repr(u8)]
pub enum Kind {
	#[tangram_serialize(id = 0)]
	ObjectCount,

	#[tangram_serialize(id = 1)]
	ObjectSize,

	#[tangram_serialize(id = 2)]
	ProcessCount,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Usage {
	pub object_count: u64,
	pub object_size: u64,
	pub process_count: u64,
}
