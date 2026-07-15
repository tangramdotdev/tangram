use tangram_client::prelude::*;

pub mod put;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Runner {
	pub scheduler: Option<tg::scheduler::Id>,
}

impl Runner {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		serde_json::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the runner"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		serde_json::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the runner"))
	}
}
