use {std::collections::BTreeSet, tangram_client::prelude::*};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Notification {
	#[tangram_serialize(id = 0)]
	BorrowableCapacity(BorrowableCapacityNotification),

	#[tangram_serialize(id = 1)]
	Heartbeat(HeartbeatNotification),

	#[tangram_serialize(id = 2)]
	SandboxEnded(SandboxEndedNotification),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct BorrowableCapacityNotification {
	#[tangram_serialize(id = 0)]
	pub available: bool,

	#[tangram_serialize(id = 1)]
	pub capacity: tg::runner::Capacity,

	#[tangram_serialize(id = 2)]
	pub heartbeat_index: u64,

	#[tangram_serialize(id = 3)]
	pub parent: tg::sandbox::Id,

	#[tangram_serialize(id = 4)]
	pub runner: tg::runner::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct HeartbeatAcknowledgement {
	#[tangram_serialize(id = 0)]
	pub registration_index: u64,

	#[tangram_serialize(id = 1)]
	pub heartbeat_index: u64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct HeartbeatNotification {
	#[tangram_serialize(id = 0)]
	pub capacity: tg::runner::control::Capacity,

	#[tangram_serialize(id = 1)]
	pub registration_index: u64,

	#[tangram_serialize(id = 2)]
	pub heartbeat_index: u64,

	#[tangram_serialize(id = 3)]
	pub runner: tg::runner::Id,

	#[tangram_serialize(id = 4)]
	pub sandboxes: BTreeSet<tg::sandbox::Id>,
}

/// A runner's report that a boot of a sandbox ended.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct SandboxEndedNotification {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub registration_index: u64,

	#[tangram_serialize(id = 2)]
	pub runner: tg::runner::Id,

	#[tangram_serialize(id = 3)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum RequestArg {
	#[tangram_serialize(id = 1)]
	AddRunner(AddRunnerRequestArg),

	#[tangram_serialize(id = 3)]
	DequeueSandbox(DequeueSandboxRequestArg),

	#[tangram_serialize(id = 4)]
	DestroyRunnerSandbox(DestroyRunnerSandboxRequestArg),

	#[tangram_serialize(id = 6)]
	EnqueueSandbox(EnqueueSandboxRequestArg),

	#[tangram_serialize(id = 8)]
	RemoveRunner(RemoveRunnerRequestArg),
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ResponseOutput {
	#[tangram_serialize(id = 1)]
	AddRunner(AddRunnerResponseOutput),

	#[tangram_serialize(id = 3)]
	DequeueSandbox(DequeueSandboxResponseOutput),

	#[tangram_serialize(id = 4)]
	DestroyRunnerSandbox(DestroyRunnerSandboxResponseOutput),

	#[tangram_serialize(id = 6)]
	EnqueueSandbox(EnqueueSandboxResponseOutput),

	#[tangram_serialize(id = 8)]
	RemoveRunner(RemoveRunnerResponseOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct AddRunnerRequestArg {
	#[tangram_serialize(id = 0)]
	pub capacity: tg::runner::control::Capacity,

	#[tangram_serialize(id = 1)]
	pub host: String,

	#[tangram_serialize(id = 2)]
	pub registration_index: u64,

	#[tangram_serialize(id = 3)]
	pub runner: tg::runner::Id,

	/// The sandboxes that the runner is running or booting, which a restarted scheduler treats as placed.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Vec::is_empty")]
	pub sandboxes: Vec<tg::runner::control::ArgSandbox>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct AddRunnerResponseOutput {
	#[tangram_serialize(id = 0)]
	pub runner: tg::runner::Id,

	#[tangram_serialize(id = 1)]
	pub scheduler: tg::scheduler::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct RemoveRunnerRequestArg {
	#[tangram_serialize(id = 0)]
	pub registration_index: u64,

	#[tangram_serialize(id = 1)]
	pub runner: tg::runner::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct RemoveRunnerResponseOutput {
	#[tangram_serialize(id = 0)]
	pub runner: tg::runner::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DequeueSandboxRequestArg {
	#[tangram_serialize(id = 0)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DequeueSandboxResponseOutput {
	#[tangram_serialize(id = 0)]
	pub dequeued: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DestroyRunnerSandboxRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub runner: tg::runner::Id,

	#[tangram_serialize(id = 2)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DestroyRunnerSandboxResponseOutput {
	#[tangram_serialize(id = 0)]
	pub destroyed: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct EnqueueSandboxRequestArg {
	#[tangram_serialize(id = 0)]
	pub arg: tg::sandbox::create::Arg,

	/// The boot to start, which is zero when the request creates the sandbox.
	#[serde(default)]
	#[tangram_serialize(default, id = 1)]
	pub boot: u64,

	#[tangram_serialize(id = 2)]
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "tg::Principal::is_root")]
	pub creator: tg::Principal,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "tg::Principal::is_root")]
	pub owner: tg::Principal,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub process: Option<tg::runner::control::Process>,

	/// The runner that the sandbox must start on, because its data is on that runner.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "Option::is_none")]
	pub runner: Option<tg::runner::Id>,

	#[tangram_serialize(id = 8)]
	pub sandbox: tg::sandbox::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 9, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<tg::scheduler::Id>,

	/// Whether to stop the sandbox as soon as it starts.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	#[tangram_serialize(default, id = 10, skip_serializing_if = "std::ops::Not::not")]
	pub stop: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 11, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct EnqueueSandboxResponseOutput {}

/// Create the data of a sandbox from its enqueued request.
#[must_use]
pub fn sandbox_data(
	request: &EnqueueSandboxRequestArg,
	status: tg::sandbox::Status,
) -> tg::sandbox::Data {
	tg::sandbox::Data {
		boot: request.boot,
		cpu: request.arg.cpu,
		creator: request.creator.clone(),
		error: None,
		expires_at: None,
		hostname: request.arg.hostname.clone(),
		id: request.sandbox.clone(),
		memory: request.arg.memory,
		mounts: request.arg.mounts.clone(),
		network: request.arg.network.clone(),
		owner: request.owner.clone(),
		status,
		stopped_at: None,
		storage: request.arg.storage,
		time_to_live: request.arg.time_to_live,
		time_to_stop: request.arg.time_to_stop,
		usage: None,
	}
}

#[must_use]
pub fn runner_heartbeat_subject(scheduler: &tg::scheduler::Id, runner: &tg::runner::Id) -> String {
	format!("schedulers.{scheduler}.runners.{runner}.heartbeat")
}

#[must_use]
pub fn scheduler_notification_subject(scheduler: &tg::scheduler::Id) -> String {
	format!("schedulers.{scheduler}.notifications")
}

impl tangram_messenger::Payload for HeartbeatAcknowledgement {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}

impl tangram_messenger::Payload for Notification {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}
