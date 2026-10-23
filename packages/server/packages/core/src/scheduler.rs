use {
	crate::{Server, Session},
	futures::future::BoxFuture,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

/// The scheduler component.
pub trait Component: crate::component::Lifecycle {
	fn destroy_runner_sandbox<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::sandbox::Id,
		reason: &'a str,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn dequeue_sandbox<'a>(
		&'a self,
		session: &'a Session,
		sandbox: &'a tg::sandbox::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<bool>>;

	fn enqueue_sandbox<'a>(
		&'a self,
		session: &'a Session,
		request: EnqueueSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<tg::scheduler::Id>>;

	fn scheduler_heartbeat_expired<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn send_scheduler_request<'a>(
		&'a self,
		session: &'a Session,
		arg: RequestArg,
	) -> BoxFuture<'a, tg::Result<(tg::scheduler::Id, tg::Result<ResponseOutput>)>>;

	fn send_scheduler_request_to<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
		arg: RequestArg,
	) -> BoxFuture<'a, tg::Result<tg::Result<ResponseOutput>>>;

	fn started(&self) -> BoxFuture<'_, ()>;
}

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

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum RequestArg {
	AddRunner(AddRunnerRequestArg),
	DequeueSandbox(DequeueSandboxRequestArg),
	EnqueueSandbox(EnqueueSandboxRequestArg),
	RemoveRunner(RemoveRunnerRequestArg),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ResponseOutput>,
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ResponseOutput {
	AddRunner(AddRunnerResponseOutput),
	Busy,
	DequeueSandbox(DequeueSandboxResponseOutput),
	EnqueueSandbox(EnqueueSandboxResponseOutput),
	RemoveRunner(RemoveRunnerResponseOutput),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatAcknowledgement {
	pub connection_index: u64,
	pub heartbeat_index: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatNotification {
	pub capacity: tg::runner::control::Capacity,
	pub connection_index: u64,
	pub heartbeat_index: u64,
	pub runner: tg::runner::Id,
	pub sandboxes: BTreeSet<tg::sandbox::Id>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AddRunnerRequestArg {
	pub capacity: tg::runner::control::Capacity,
	pub host: String,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AddRunnerResponseOutput {
	pub connection_index: u64,
	pub runner: tg::runner::Id,
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoveRunnerRequestArg {
	pub connection_index: u64,
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
pub struct EnqueueSandboxRequestArg {
	pub arg: tg::sandbox::create::Arg,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process: Option<tg::runner::control::Process>,

	pub sandbox: tg::sandbox::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<tg::scheduler::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EnqueueSandboxResponseOutput {
	pub enqueued: bool,
}

#[must_use]
pub fn runner_heartbeat_subject(scheduler: &tg::scheduler::Id, runner: &tg::runner::Id) -> String {
	format!("schedulers.{scheduler}.runners.{runner}.heartbeat")
}

#[must_use]
pub fn scheduler_heartbeat_subject(scheduler: &tg::scheduler::Id) -> String {
	format!("schedulers.{scheduler}.heartbeat")
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
