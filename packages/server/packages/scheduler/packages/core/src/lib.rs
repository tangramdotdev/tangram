use {std::collections::BTreeSet, tangram_client::prelude::*};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Message {
	Ack(Ack),
	Notification(Notification),
	Request(Request),
	Response(Response),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Ack {
	pub id: String,
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Notification {
	BorrowableCapacity(BorrowableCapacityNotification),
	Heartbeat(HeartbeatNotification),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BorrowableCapacityNotification {
	pub available: bool,
	pub capacity: tg::runner::Capacity,
	pub heartbeat_index: u64,
	pub parent: tg::sandbox::Id,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
	pub arg: RequestArg,
	pub id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ResponseOutput>,
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatAcknowledgement {
	pub registration_index: u64,
	pub heartbeat_index: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatNotification {
	pub capacity: tg::runner::control::Capacity,
	pub registration_index: u64,
	pub heartbeat_index: u64,
	pub runner: tg::runner::Id,
	pub sandboxes: BTreeSet<tg::sandbox::Id>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum RequestArg {
	AddRunner(AddRunnerRequestArg),
	DequeueSandbox(DequeueSandboxRequestArg),
	DestroyRunnerSandbox(DestroyRunnerSandboxRequestArg),
	EndSandbox(EndSandboxRequestArg),
	EnqueueSandbox(EnqueueSandboxRequestArg),
	RemoveRunner(RemoveRunnerRequestArg),
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ResponseOutput {
	AddRunner(AddRunnerResponseOutput),
	Busy,
	DequeueSandbox(DequeueSandboxResponseOutput),
	DestroyRunnerSandbox(DestroyRunnerSandboxResponseOutput),
	EndSandbox(EndSandboxResponseOutput),
	EnqueueSandbox(EnqueueSandboxResponseOutput),
	RemoveRunner(RemoveRunnerResponseOutput),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AddRunnerRequestArg {
	pub capacity: tg::runner::control::Capacity,
	pub host: String,
	pub registration_index: u64,
	pub runner: tg::runner::Id,

	/// The sandboxes that the runner is running or booting, which a restarted scheduler treats as placed.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub sandboxes: Vec<tg::runner::control::ArgSandbox>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AddRunnerResponseOutput {
	pub runner: tg::runner::Id,
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoveRunnerRequestArg {
	pub registration_index: u64,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoveRunnerResponseOutput {
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DequeueSandboxRequestArg {
	pub sandbox: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DequeueSandboxResponseOutput {
	pub dequeued: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DestroyRunnerSandboxRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	pub runner: tg::runner::Id,
	pub sandbox: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DestroyRunnerSandboxResponseOutput {
	pub destroyed: bool,
}

/// A stop or destroy of a sandbox that the scheduler owns, because its runner has not connected.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EndSandboxRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	pub sandbox: tg::sandbox::Id,

	/// Whether to stop the sandbox rather than destroy it. A sandbox that a runner already holds cannot be stopped until it connects.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub stop: bool,
}

/// The output is `false` if this scheduler does not own the sandbox.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EndSandboxResponseOutput {
	pub ended: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EnqueueSandboxRequestArg {
	pub arg: tg::sandbox::create::Arg,

	/// The boot to start, which is zero when the request creates the sandbox.
	#[serde(default)]
	pub boot: u64,

	pub created_at: i64,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	pub creator: tg::Principal,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	pub owner: tg::Principal,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process: Option<tg::runner::control::Process>,

	/// The runner that the sandbox must start on, because its data is on that runner.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runner: Option<tg::runner::Id>,

	pub sandbox: tg::sandbox::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<tg::scheduler::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
pub fn scheduler_server_subject(scheduler: Option<&tg::scheduler::Id>) -> String {
	scheduler.map_or_else(
		|| "schedulers.server".to_owned(),
		|scheduler| format!("schedulers.{scheduler}.server"),
	)
}

impl tangram_messenger::Payload for Message {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(message)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}
