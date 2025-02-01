use crate::{self as tg, handle::Ext as _, util::arc::Ext as _};
use std::{
	collections::BTreeMap,
	ops::Deref,
	path::PathBuf,
	sync::{Arc, RwLock},
};

pub use self::{
	id::Id,
	status::Status,
	wait::{Exit, Wait},
};

pub mod children;
pub mod dequeue;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod id;
pub mod log;
pub mod pull;
pub mod push;
pub mod put;
pub mod spawn;
pub mod start;
pub mod status;
pub mod touch;
pub mod wait;

#[derive(Clone, Debug)]
pub struct Process(Arc<Inner>);

#[derive(Debug)]
pub struct Inner {
	id: Id,
	remote: Option<String>,
	state: RwLock<Option<Arc<State>>>,
	token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct State {
	pub checksum: Option<tg::Checksum>,
	pub command: tg::Command,
	pub commands_complete: bool,
	pub commands_count: Option<u64>,
	pub commands_depth: Option<u64>,
	pub commands_weight: Option<u64>,
	pub complete: bool,
	pub count: Option<u64>,
	pub created_at: time::OffsetDateTime,
	pub cwd: Option<PathBuf>,
	pub dequeued_at: Option<time::OffsetDateTime>,
	pub enqueued_at: Option<time::OffsetDateTime>,
	pub env: Option<BTreeMap<String, String>>,
	pub error: Option<tg::Error>,
	pub exit: Option<tg::process::Exit>,
	pub finished_at: Option<time::OffsetDateTime>,
	pub heartbeat_at: Option<time::OffsetDateTime>,
	pub log: Option<tg::Blob>,
	pub logs_complete: bool,
	pub logs_count: Option<u64>,
	pub logs_depth: Option<u64>,
	pub logs_weight: Option<u64>,
	pub network: bool,
	pub output: Option<tg::Value>,
	pub outputs_complete: bool,
	pub outputs_count: Option<u64>,
	pub outputs_depth: Option<u64>,
	pub outputs_weight: Option<u64>,
	pub retry: bool,
	pub started_at: Option<time::OffsetDateTime>,
	pub status: tg::process::Status,
	pub touched_at: Option<time::OffsetDateTime>,
}

impl Process {
	#[must_use]
	pub fn new(
		id: Id,
		remote: Option<String>,
		state: Option<State>,
		token: Option<String>,
	) -> Self {
		let state = RwLock::new(state.map(Arc::new));
		Self(Arc::new(Inner {
			id,
			remote,
			state,
			token,
		}))
	}

	#[must_use]
	pub fn id(&self) -> &Id {
		&self.id
	}

	#[must_use]
	pub fn remote(&self) -> Option<&String> {
		self.remote.as_ref()
	}

	#[must_use]
	pub fn state(&self) -> &RwLock<Option<Arc<State>>> {
		&self.state
	}

	#[must_use]
	pub fn token(&self) -> Option<&String> {
		self.token.as_ref()
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Arc<tg::process::State>>
	where
		H: tg::Handle,
	{
		self.try_load(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the process"))
	}

	pub async fn try_load<H>(&self, handle: &H) -> tg::Result<Option<Arc<tg::process::State>>>
	where
		H: tg::Handle,
	{
		if let Some(state) = self.state.read().unwrap().clone() {
			return Ok(Some(state));
		}
		let Some(output) = handle.try_get_process(self.id()).await? else {
			return Ok(None);
		};
		let state: tg::process::State = output.try_into()?;
		let state = Arc::new(state);
		self.state.write().unwrap().replace(state.clone());
		Ok(Some(state))
	}

	pub async fn command<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = tg::Command>>
	where
		H: tg::Handle,
	{
		Ok(self.load(handle).await?.map(|state| &state.command))
	}

	pub async fn retry<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = bool>>
	where
		H: tg::Handle,
	{
		Ok(self.load(handle).await?.map(|state| &state.retry))
	}

	pub async fn spawn<H>(handle: &H, arg: tg::process::spawn::Arg) -> tg::Result<tg::Process>
	where
		H: tg::Handle,
	{
		let output = handle.spawn_process(arg).await?;
		let process = tg::Process::new(output.process, output.remote, None, None);
		Ok(process)
	}

	pub async fn wait<H>(&self, handle: &H) -> tg::Result<tg::process::Wait>
	where
		H: tg::Handle,
	{
		handle.wait_process(&self.id).await?.try_into()
	}

	pub async fn run<H>(handle: &H, arg: tg::process::spawn::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let process = Self::spawn(handle, arg).await?;
		let output = process.wait(handle).await?;
		if let Some(error) = output.error {
			return Err(error);
		}
		if let Some(output) = output.output {
			return Ok(output);
		}
		Err(tg::error!("invalid output"))
	}
}

impl Deref for Process {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl TryFrom<tg::process::get::Output> for tg::process::State {
	type Error = tg::Error;

	fn try_from(value: tg::process::get::Output) -> Result<Self, Self::Error> {
		let checksum = value.checksum;
		let command = tg::Command::with_id(value.command);
		let commands_complete = value.commands_complete;
		let commands_count = value.commands_count;
		let commands_depth = value.commands_depth;
		let commands_weight = value.commands_weight;
		let complete = value.complete;
		let count = value.count;
		let created_at = value.created_at;
		let cwd = value.cwd;
		let dequeued_at = value.dequeued_at;
		let enqueued_at = value.enqueued_at;
		let env = value.env;
		let error = value.error;
		let exit = value.exit;
		let finished_at = value.finished_at;
		let heartbeat_at = value.heartbeat_at;
		let log = value.log.map(tg::Blob::with_id);
		let logs_complete = value.logs_complete;
		let logs_count = value.logs_count;
		let logs_depth = value.logs_depth;
		let logs_weight = value.logs_weight;
		let network = value.network;
		let output = value.output.map(tg::Value::try_from).transpose()?;
		let outputs_complete = value.outputs_complete;
		let outputs_count = value.outputs_count;
		let outputs_depth = value.outputs_depth;
		let outputs_weight = value.outputs_weight;
		let retry = value.retry;
		let started_at = value.started_at;
		let status = value.status;
		let touched_at = value.touched_at;
		Ok(State {
			checksum,
			command,
			commands_complete,
			commands_count,
			commands_depth,
			commands_weight,
			complete,
			count,
			created_at,
			cwd,
			dequeued_at,
			enqueued_at,
			env,
			error,
			exit,
			finished_at,
			heartbeat_at,
			log,
			logs_complete,
			logs_count,
			logs_depth,
			logs_weight,
			network,
			output,
			outputs_complete,
			outputs_count,
			outputs_depth,
			outputs_weight,
			retry,
			started_at,
			status,
			touched_at,
		})
	}
}
