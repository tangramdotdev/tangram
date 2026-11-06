use tangram_client::prelude::*;

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Registration {
	#[tangram_serialize(id = 0)]
	pub index: u64,

	#[tangram_serialize(id = 1)]
	pub runner: tg::runner::Id,

	#[tangram_serialize(id = 2)]
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SchedulerSandbox {
	pub id: tg::sandbox::Id,
	pub request: Vec<u8>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Runner {
	#[tangram_serialize(id = 0)]
	pub next: u64,

	#[tangram_serialize(id = 1)]
	pub registration: u64,

	#[tangram_serialize(id = 2)]
	pub scheduler: Option<tg::scheduler::Id>,
}

impl Default for Runner {
	fn default() -> Self {
		Self {
			next: 1,
			registration: 0,
			scheduler: None,
		}
	}
}
