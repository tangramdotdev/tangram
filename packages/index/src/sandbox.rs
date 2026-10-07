use {futures::FutureExt as _, tangram_client::prelude::*};

pub mod put;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Sandbox {
	pub account: Option<crate::usage::Account>,
	pub created_at: i64,
	pub data: Option<tg::sandbox::get::Output>,
	pub location: Option<tg::Location>,
	pub reference_count: u64,
	pub runner: Option<tg::runner::Id>,
	pub set: Set,
	pub touched_at: i64,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Set {
	pub processes: bool,
}

pub trait Index {
	fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>>> + Send;

	fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>>> + Send;

	fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> impl Future<Output = tg::Result<Vec<tg::sandbox::Id>>> + Send;

	fn try_get_sandbox_processes_count(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<Option<u64>>> + Send;

	fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> impl Future<Output = tg::Result<Option<Vec<tg::process::Id>>>> + Send;

	fn get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> impl Future<Output = tg::Result<Vec<tg::process::Id>>> + Send {
		self.try_get_sandbox_processes(id, position, length)
			.map(|result| {
				result.and_then(|option| {
					option.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))
				})
			})
	}

	fn list_sandboxes(
		&self,
	) -> impl Future<Output = tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>>> + Send;

	fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::sandbox::Sandbox>>>> + Send;

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<Option<crate::sandbox::Sandbox>>> + Send {
		self.try_get_sandboxes(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}
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
