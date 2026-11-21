use {
	crate::prelude::*,
	futures::stream::BoxStream,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::{collections::BTreeSet, ops::ControlFlow, time::Duration},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_default,
};

pub const TANGRAM_CONTENT_TYPE: &str = "application/vnd.tangram.runner-control";

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientMessage {
	#[tangram_serialize(id = 0)]
	Ack(ClientAck),

	#[tangram_serialize(id = 1)]
	Notification(ClientNotification),

	#[tangram_serialize(id = 2)]
	Request(ClientRequest),

	#[tangram_serialize(id = 3)]
	Response(ClientResponse),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerMessage {
	#[tangram_serialize(id = 0)]
	Ack(ServerAck),

	#[tangram_serialize(id = 1)]
	Request(ServerRequest),

	#[tangram_serialize(id = 2)]
	Response(ServerResponse),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ClientAck {
	#[tangram_serialize(id = 0)]
	pub id: String,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientNotification {
	/// The last message of a runner that shuts down, after which the server removes the runner at once.
	#[tangram_serialize(id = 2)]
	End,

	#[tangram_serialize(id = 0)]
	Heartbeat(HeartbeatClientNotification),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ClientRequest {
	#[tangram_serialize(id = 0)]
	pub arg: ClientRequestArg,

	#[tangram_serialize(id = 1)]
	pub id: String,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientRequestArg {
	#[tangram_serialize(id = 3)]
	AddSandbox(AddSandboxClientRequestArg),

	#[tangram_serialize(id = 0)]
	DestroySandbox(DestroySandboxClientRequestArg),

	#[tangram_serialize(id = 5)]
	EndSandbox(EndSandboxClientRequestArg),

	#[tangram_serialize(id = 4)]
	FinishProcesses(FinishProcessesClientRequestArg),

	#[tangram_serialize(id = 2)]
	ReportSandboxUsage(ReportSandboxUsageClientRequestArg),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ClientResponse {
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub id: String,

	#[tangram_serialize(id = 2)]
	pub output: Option<ClientResponseOutput>,
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
pub enum ClientResponseOutput {
	#[tangram_serialize(id = 1)]
	DestroySandbox(DestroySandboxClientResponseOutput),

	#[tangram_serialize(id = 0)]
	OfferSandbox(OfferSandboxClientResponseOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ServerAck {
	#[tangram_serialize(id = 0)]
	pub id: String,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ServerRequest {
	#[tangram_serialize(id = 0)]
	pub arg: ServerRequestArg,

	#[tangram_serialize(id = 1)]
	pub id: String,
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
pub enum ServerRequestArg {
	#[tangram_serialize(id = 1)]
	DestroySandbox(DestroySandboxServerRequestArg),

	#[tangram_serialize(id = 0)]
	OfferSandbox(OfferSandboxServerRequestArg),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ServerResponse {
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub id: String,

	#[tangram_serialize(id = 2)]
	pub output: Option<ServerResponseOutput>,

	/// The server's request that the runner send the request again later, because the server cannot decide it yet, such as a report of a boot that a restarted scheduler does not know until it recovers.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub retry: Option<tg::control::Retry>,
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
pub enum ServerResponseOutput {
	#[tangram_serialize(id = 3)]
	AddSandbox(AddSandboxServerResponseOutput),

	#[tangram_serialize(id = 0)]
	DestroySandbox(DestroySandboxServerResponseOutput),

	#[tangram_serialize(id = 5)]
	EndSandbox(EndSandboxServerResponseOutput),

	#[tangram_serialize(id = 4)]
	FinishProcesses(FinishProcessesServerResponseOutput),

	#[tangram_serialize(id = 2)]
	ReportSandboxUsage(ReportSandboxUsageServerResponseOutput),
}

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct HeartbeatClientNotification {
	#[tangram_serialize(id = 0)]
	pub capacity: Capacity,

	#[tangram_serialize(id = 1)]
	pub index: u64,

	#[tangram_serialize(id = 2)]
	pub sandboxes: BTreeSet<tg::sandbox::Id>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Capacity {
	#[tangram_serialize(id = 0)]
	pub available: tg::runner::Capacity,

	/// The maximum number of hardware threads that one sandbox can use.
	#[tangram_serialize(id = 1)]
	pub cpu_limit: u64,

	/// The maximum overcommit factor for each resource.
	#[tangram_serialize(id = 2)]
	pub overcommit: tg::runner::Overcommit,

	#[tangram_serialize(id = 3)]
	pub total: tg::runner::Capacity,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Process {
	#[tangram_serialize(id = 0)]
	pub data: tg::process::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::process::Id>,

	#[tangram_serialize(id = 6)]
	pub lease: tg::process::Lease,

	/// The leases that callers acquired from the scheduler while the process was queued.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "Vec::is_empty")]
	pub leases: Vec<tg::process::Lease>,

	/// The stdio opens that the runner services when the process starts, as if each arrived over process control.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Vec::is_empty")]
	pub open: Vec<tg::process::control::ServerRequest>,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "is_default")]
	pub options: tg::referent::Options,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
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
pub struct OfferSandboxServerRequestArg {
	#[tangram_serialize(id = 0)]
	pub arg: tg::sandbox::create::Arg,

	/// The boot to start, which is zero when the request creates the sandbox.
	#[serde(default, skip_serializing_if = "tangram_util::serde::is_default")]
	#[tangram_serialize(
		default,
		id = 9,
		skip_serializing_if = "tangram_util::serde::is_default"
	)]
	pub boot: u64,

	#[tangram_serialize(id = 2)]
	pub capacity: tg::runner::Capacity,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "tg::Principal::is_root")]
	pub creator: tg::Principal,

	/// The parent whose capacity the sandbox borrows, if it borrows.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub lender: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	#[tangram_serialize(id = 8, default, skip_serializing_if = "tg::Principal::is_root")]
	pub owner: tg::Principal,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
	pub process: Option<Process>,

	#[tangram_serialize(id = 6)]
	pub sandbox: tg::sandbox::Id,

	/// The sequence of the scheduler's last write of the boot before the offer, after which the scheduler's handoff write and then the runner's writes follow.
	#[serde(default, skip_serializing_if = "tangram_util::serde::is_default")]
	#[tangram_serialize(
		id = 11,
		default,
		skip_serializing_if = "tangram_util::serde::is_default"
	)]
	pub sequence: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "Option::is_none")]
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
pub struct OfferSandboxClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub accepted: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DestroySandboxClientRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

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
pub struct DestroySandboxServerResponseOutput {
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
pub struct DestroySandboxServerRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

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
pub struct DestroySandboxClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub destroyed: bool,
}

/// A runner's report of a sandbox that it started on the shortcut path, so that the runner's scheduler knows the sandbox and sends the spawns into it.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct AddSandboxClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	/// The proof that the runner derived the sandbox's ID.
	#[tangram_serialize(id = 2)]
	pub proof: tg::sandbox::id::Proof,

	#[tangram_serialize(id = 1)]
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
pub struct AddSandboxServerResponseOutput {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageClientRequestArg {
	/// The stopped sandboxes that the report covers.
	#[tangram_serialize(id = 0)]
	pub sandboxes: Vec<ReportSandboxUsageClientRequestSandbox>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageClientRequestSandbox {
	/// The boot that the sandbox stopped after.
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,

	/// The storage usage of the stopped directory since the boot stopped, if the runner measured it.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub usage: Option<tg::sandbox::Usage>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageServerResponseOutput {
	/// What the runner does with each sandbox of the report, in the report's order.
	#[tangram_serialize(id = 0)]
	pub sandboxes: Vec<ReportSandboxUsageServerResponseSandbox>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageServerResponseSandbox {
	#[tangram_serialize(id = 0)]
	pub action: StoppedSandboxAction,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,
}

/// What a runner does with the directory of a stopped sandbox that it reported.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum StoppedSandboxAction {
	/// Keep the directory.
	#[tangram_serialize(id = 0)]
	Keep,

	/// Remove the directory and report the sandbox's destruction.
	#[tangram_serialize(id = 1)]
	Remove(StoppedSandboxRemoval),
}

/// The reason that a runner removes the directory of a stopped sandbox.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum StoppedSandboxRemoval {
	/// The index records the sandbox as destroyed.
	#[tangram_serialize(id = 0)]
	Destroyed,

	/// The index records the sandbox as destroying, and the directory is all that is left of it.
	#[tangram_serialize(id = 1)]
	Destroying,

	/// The index records the sandbox as held by another runner.
	#[tangram_serialize(id = 2)]
	OtherRunner,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct FinishProcessesClientRequestArg {
	/// The boot that the scheduler offered the processes into.
	#[tangram_serialize(id = 2)]
	pub boot: u64,

	#[tangram_serialize(id = 0)]
	pub error: tg::Either<tg::error::Data, tg::error::Id>,

	/// The processes that the runner accepted and that cannot run, from the runner's memory, which the server finishes as failed with the error if the scheduler offered them into the boot.
	#[tangram_serialize(id = 1)]
	pub processes: Vec<Process>,

	#[tangram_serialize(id = 3)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct FinishProcessesServerResponseOutput {}

/// A runner's report that a boot that it held ended, which is stopped or destroyed, with the processes that the runner accepted for the boot and that did not finish, from the runner's memory.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct EndSandboxClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub created_at: i64,

	/// The sandbox's data at the end of the boot, with its status and error.
	#[tangram_serialize(id = 2)]
	pub data: tg::sandbox::get::Output,

	/// The processes of the boot.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Vec::is_empty")]
	pub processes: Vec<tg::process::Id>,

	/// The proof that the runner derived the sandbox's ID, for a sandbox that the runner started on the shortcut path.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Option::is_none")]
	pub proof: Option<tg::sandbox::id::Proof>,

	/// Whether the runner recorded the end at the sandbox's home through the boot's control stream, in which case the server does not record it again.
	#[serde(default, skip_serializing_if = "tangram_util::serde::is_default")]
	#[tangram_serialize(
		default,
		id = 8,
		skip_serializing_if = "tangram_util::serde::is_default"
	)]
	pub recorded: bool,

	#[tangram_serialize(id = 5)]
	pub sandbox: tg::sandbox::Id,

	/// The sequence of the runner's write of the end among its writes of the boot.
	#[serde(default, skip_serializing_if = "tangram_util::serde::is_default")]
	#[tangram_serialize(
		default,
		id = 6,
		skip_serializing_if = "tangram_util::serde::is_default"
	)]
	pub sequence: u64,

	/// The processes that the runner accepted for the boot and that did not finish, which the server finishes as failed with the boot's error.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "Vec::is_empty")]
	pub unfinished: Vec<Process>,
}

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct EndSandboxServerResponseOutput {}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Arg {
	/// The sandboxes that the runner has directories for or is running, which it reports on every connection, so that the scheduler decides which directories the runner keeps.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Vec::is_empty")]
	pub directories: Vec<tg::sandbox::Id>,

	/// Whether this is the first connection of the runner's program since it started, in which case the scheduler takes over the boots that the runner no longer holds.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "std::ops::Not::not")]
	pub first: bool,

	#[tangram_serialize(id = 0)]
	pub heartbeat: HeartbeatClientNotification,

	#[tangram_serialize(id = 1)]
	pub host: String,

	#[tangram_serialize(id = 2)]
	pub id: tg::runner::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	/// The sandboxes that the runner is running.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Vec::is_empty")]
	pub sandboxes: Vec<ArgSandbox>,

	#[serde_as(as = "DurationSecondsWithFrac")]
	#[tangram_serialize(id = 4)]
	#[serde(alias = "scheduler_ttl")]
	pub scheduler_time_to_live: Duration,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ArgSandbox {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub id: tg::sandbox::Id,

	/// The processes that the sandbox's scheduler offered into the boot and that the runner holds, which a restarted scheduler cancels if the index does not have them.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Vec::is_empty")]
	pub processes: Vec<tg::process::Id>,

	/// The proof that the runner derived the sandbox's ID, for a boot that the runner started on the shortcut path, which a restarted scheduler does not cancel although the index may not have it yet.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub proof: Option<tg::sandbox::id::Proof>,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Header {
	/// The sandboxes with directories that the runner reported on its first connection after it started, which the connection's takeover destroyed.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Vec::is_empty")]
	pub destroyed: Vec<tg::sandbox::Id>,

	/// The reported boots that the index records as ended or replaced by a later boot, such as by a takeover, which the runner gives up without writing anything more for them.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub ended: Vec<tg::sandbox::Id>,

	/// The reported sandboxes whose directories the runner keeps, because the sandboxes are stopped on the runner or their boots wait in the scheduler's queue. The runner removes every other directory that it reported, except the directories of the boots that it still holds.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Vec::is_empty")]
	pub kept: Vec<tg::sandbox::Id>,

	/// How long the scheduler keeps the runner after its last heartbeat, after which the scheduler no longer sends the runner a request again.
	#[serde_as(as = "DurationSecondsWithFrac")]
	#[tangram_serialize(id = 2)]
	#[serde(alias = "runner_ttl")]
	pub runner_time_to_live: Duration,

	#[tangram_serialize(id = 1)]
	pub scheduler: tg::scheduler::Id,

	/// The sandboxes with directories that the runner reported on its first connection after it started, which the connection's takeover stopped. The runner keeps their directories, which `kept` also lists.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Vec::is_empty")]
	pub stopped: Vec<tg::sandbox::Id>,
}

impl tg::Session {
	pub async fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::runner::control::Header,
				impl futures::Stream<Item = tg::Result<tg::runner::control::ServerMessage>>
				+ Send
				+ 'static
				+ use<>,
			),
			tg::Error,
		>,
	> {
		let method = http::Method::POST;
		let uri = Uri::builder().path("/runners/control").build().unwrap();
		let max_frame_size = self.client().sync.max_frame_size;
		let body = crate::frame::encode(stream, max_frame_size);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE)
			.arg_with_tangram(&arg, body)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = match self
			.send_attempt(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?
		{
			ControlFlow::Break(response) => response,
			ControlFlow::Continue(error) => {
				let error = tg::error!(!error, "failed to send the request");
				return Ok(ControlFlow::Continue(error));
			},
		};
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if content_type != Some(TANGRAM_CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}

		// Decode the response, which ends early if the connection is lost.
		let Some((header, stream)) = crate::frame::decode_with_header(
			response.into_body(),
			self.client().http.max_header_length,
			max_frame_size,
		)
		.await?
		else {
			let error = tg::error!("the runner control stream ended before its header");
			return Ok(ControlFlow::Continue(error));
		};

		Ok(ControlFlow::Break((header, stream)))
	}
}

impl TryFrom<ClientMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: ClientMessage) -> Result<Self, Self::Error> {
		let event = match value {
			ClientMessage::Ack(ack) => {
				let data = serde_json::to_string(&ack)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("ack".to_owned()),
					..Default::default()
				}
			},
			ClientMessage::Notification(notification) => {
				let data = serde_json::to_string(&notification)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("notification".to_owned()),
					..Default::default()
				}
			},
			ClientMessage::Request(request) => {
				let data = serde_json::to_string(&request)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("request".to_owned()),
					..Default::default()
				}
			},
			ClientMessage::Response(response) => {
				let data = serde_json::to_string(&response)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("response".to_owned()),
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ClientMessage {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("ack") => {
				let ack = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Ack(ack))
			},
			Some("notification") => {
				let notification = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Notification(notification))
			},
			Some("request") => {
				let request = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Request(request))
			},
			Some("response") => {
				let response = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Response(response))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid message")),
		}
	}
}

impl TryFrom<ServerMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: ServerMessage) -> Result<Self, Self::Error> {
		let event = match value {
			ServerMessage::Request(request) => {
				let data = serde_json::to_string(&request)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("request".to_owned()),
					..Default::default()
				}
			},
			ServerMessage::Ack(ack) => {
				let data = serde_json::to_string(&ack)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("ack".to_owned()),
					..Default::default()
				}
			},
			ServerMessage::Response(response) => {
				let data = serde_json::to_string(&response)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("response".to_owned()),
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ServerMessage {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("ack") => {
				let ack = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Ack(ack))
			},
			Some("request") => {
				let request = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Request(request))
			},
			Some("response") => {
				let response = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Response(response))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid message")),
		}
	}
}
