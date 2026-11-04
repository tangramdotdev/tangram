use tangram_client::prelude::*;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Registration {
	#[tangram_serialize(id = 0)]
	pub attempt: u64,

	#[tangram_serialize(id = 1)]
	pub runner: tg::runner::Id,

	#[tangram_serialize(id = 2)]
	pub scheduler: tg::scheduler::Id,
}
