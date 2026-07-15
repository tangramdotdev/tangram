use tangram_client::prelude::*;

pub mod put;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Sandbox {
	pub created_at: i64,
	pub data: Option<tg::sandbox::get::Output>,
	pub runner: Option<tg::runner::Id>,
}

impl Sandbox {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		serde_json::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the sandbox"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		serde_json::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the sandbox"))
	}
}
