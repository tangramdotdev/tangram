use {
	futures::FutureExt as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_util::serde::{is_default, is_false},
};

pub use tg::process::storage;

pub mod object;
pub mod put;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Process {
	/// The command identity, which need not identify a stored object.
	#[tangram_serialize(id = 8)]
	pub command_id: tg::object::Id,

	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub data: Option<tg::process::Data>,

	#[tangram_serialize(id = 7)]
	pub location: Option<tg::Location>,

	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub metadata: tg::process::Metadata,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<tg::sandbox::Id>,

	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_default")]
	pub set: Set,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub storage: storage::Set,

	#[tangram_serialize(id = 3)]
	pub touched_at: i64,
}

#[derive(Clone, Debug)]
pub struct NodeChildren {
	pub complete: bool,
	pub nodes: Vec<tg::Referent<tg::Id>>,
}

/// The set status of a process in the index.
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Set {
	/// Whether the complete children list for this node is set.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub children: bool,

	/// Whether the complete command object list is set, including an empty list.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_false")]
	pub command_objects: bool,

	/// Whether the complete error object list is set, including an empty list.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub error_objects: bool,

	/// Whether the complete log object list is set, including an empty list.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_false")]
	pub log_objects: bool,

	/// Whether the complete output object list is set, including an empty list.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_false")]
	pub output_objects: bool,
}

pub trait Index {
	fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn try_get_process_children_count(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<u64>>> + Send;

	fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> impl Future<Output = tg::Result<Option<Vec<tg::process::data::Child>>>> + Send;

	fn try_get_process_children_and_objects(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<crate::process::NodeChildren>>> + Send;

	fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Vec<(tg::process::Id, crate::process::Process)>>> + Send;

	fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> impl Future<Output = tg::Result<bool>> + Send;

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.try_get_processes(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &crate::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn touch_process(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.touch_processes(std::slice::from_ref(id), touched_at, time_to_touch)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_process_and_put_account(
		&self,
		id: &tg::process::Id,
		account: &crate::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.touch_processes_and_put_account(
			std::slice::from_ref(id),
			account,
			touched_at,
			time_to_touch,
		)
		.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_process_with_account(
		&self,
		id: &tg::process::Id,
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.touch_processes_with_account(
			std::slice::from_ref(id),
			account,
			touched_at,
			time_to_touch,
		)
		.map(|result| result.map(|mut output| output.pop().unwrap()))
	}
}

impl Process {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the process"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the process"))
	}
}

impl Set {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.children
			&& self.command_objects
			&& self.error_objects
			&& self.log_objects
			&& self.output_objects
	}

	pub fn merge(&mut self, other: &Self) {
		self.children = self.children || other.children;
		self.command_objects = self.command_objects || other.command_objects;
		self.error_objects = self.error_objects || other.error_objects;
		self.log_objects = self.log_objects || other.log_objects;
		self.output_objects = self.output_objects || other.output_objects;
	}
}
