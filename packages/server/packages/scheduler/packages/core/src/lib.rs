use tangram_client::prelude::*;

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum RequestArg {
	AddRunner(AddRunnerRequestArg),
	DequeueSandbox(DequeueSandboxRequestArg),
	EnqueueSandbox(EnqueueSandboxRequestArg),
	RemoveRunner(RemoveRunnerRequestArg),
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
