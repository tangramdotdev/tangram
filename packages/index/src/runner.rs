use tangram_client::prelude::*;

pub mod put;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SchedulerSandbox {
	pub id: tg::sandbox::Id,
	pub request: Vec<u8>,
}

/// The record of a runner, which names the scheduler that the runner belongs to for its whole life.
#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Runner {
	#[tangram_serialize(id = 2)]
	pub scheduler: Option<tg::scheduler::Id>,
}
