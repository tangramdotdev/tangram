use {futures::FutureExt as _, std::collections::BTreeMap, tangram_client::prelude::*};

pub mod list;
pub mod put;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Sandbox {
	pub account: Option<crate::usage::Account>,

	pub created_at: i64,

	pub data: Option<tg::sandbox::get::Output>,

	pub location: Option<tg::Location>,

	pub reference_count: u64,

	pub runner: Option<tg::runner::Id>,

	/// The scheduler that owns the sandbox until its runner connects.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<tg::scheduler::Id>,

	pub set: Set,

	/// The storage usage of each boot's stopped directory, which the usage in `data` includes.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub stopped_usages: BTreeMap<u64, tg::sandbox::Usage>,

	pub touched_at: i64,

	/// The usage reported for each boot, which sums with the stopped usage to the usage in `data`.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub usages: BTreeMap<u64, tg::sandbox::Usage>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Set {
	pub processes: bool,
}

pub trait Index {
	fn list_sandboxes_for_creator(
		&self,
		arg: crate::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<crate::sandbox::list::Output>> + Send;

	fn list_sandboxes_for_owner(
		&self,
		arg: crate::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<crate::sandbox::list::Output>> + Send;

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
			.map(move |result| {
				result.and_then(|option| {
					option.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))
				})
			})
	}

	fn list_sandbox_ids(
		&self,
		after: Option<tg::sandbox::Id>,
		limit: usize,
	) -> impl Future<Output = tg::Result<Vec<tg::sandbox::Id>>> + Send;

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
