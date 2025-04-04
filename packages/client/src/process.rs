use crate::{self as tg, handle::Ext as _, host, util::arc::Ext as _};
use std::{
	ops::Deref,
	str::FromStr as _,
	sync::{Arc, RwLock},
};

pub use self::{
	data::Data,
	id::Id,
	metadata::Metadata,
	mount::Mount,
	signal::Signal,
	state::State,
	status::Status,
	stdio::Stdio,
	wait::{Exit, Wait},
};

pub mod children;
pub mod data;
pub mod dequeue;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod id;
pub mod log;
pub mod metadata;
pub mod mount;
pub mod put;
pub mod signal;
pub mod spawn;
pub mod start;
pub mod state;
pub mod status;
pub mod stdio;
pub mod touch;
pub mod wait;

#[derive(Clone, Debug)]
pub struct Process(Arc<Inner>);

#[derive(Debug)]
pub struct Inner {
	id: Id,
	remote: Option<String>,
	state: RwLock<Option<Arc<State>>>,
	metadata: RwLock<Option<Arc<Metadata>>>,
	token: Option<String>,
}

impl Process {
	#[must_use]
	pub fn new(
		id: Id,
		remote: Option<String>,
		state: Option<State>,
		metadata: Option<Metadata>,
		token: Option<String>,
	) -> Self {
		let state = RwLock::new(state.map(Arc::new));
		let metadata = RwLock::new(metadata.map(Arc::new));
		Self(Arc::new(Inner {
			id,
			remote,
			state,
			metadata,
			token,
		}))
	}

	pub async fn current<H>(handle: &H) -> tg::Result<Option<Self>>
	where
		H: tg::Handle,
	{
		let id = std::env::var("TANGRAM_PROCESS")
			.map_err(|source| tg::error!(!source, "unable to locate current process ID"))?;
		let id = tg::process::Id::from_str(&id)
			.map_err(|source| tg::error!(!source, %id, "could not parse process ID"))?;
		let output = handle
			.try_get_process(&id)
			.await
			.map_err(|source| tg::error!(!source, %id, "unable to look up process"))?;
		let Some(output) = output else {
			return Ok(None);
		};
		let state = tg::process::State::try_from(output.data)?;
		let process = Self::new(id, None, Some(state), None, None);
		Ok(Some(process))
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
	pub fn metadata(&self) -> &RwLock<Option<Arc<Metadata>>> {
		&self.metadata
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
		let state = tg::process::State::try_from(output.data)?;
		let state = Arc::new(state);
		self.state.write().unwrap().replace(state.clone());
		Ok(Some(state))
	}

	pub async fn command<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = tg::Command> + use<H>>
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
		let process = tg::Process::new(output.process, output.remote, None, None, None);
		Ok(process)
	}

	pub async fn wait<H>(&self, handle: &H) -> tg::Result<tg::process::Wait>
	where
		H: tg::Handle,
	{
		handle.wait_process(&self.id).await?.try_into()
	}

	pub async fn build<H>(handle: &H, arg: tg::process::spawn::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let arg = if let Some(current) = Self::current(handle).await? {
			// Check current process for mounts, merge with arg mounts.
			let state = current.state().read().unwrap().clone();
			let mounts = state.as_ref().map(|state| state.mounts.clone());
			let mut current_mount_data = vec![];
			if let Some(mounts) = &mounts {
				for mount in mounts {
					let mount_data = mount.data(handle).await?;
					current_mount_data.push(mount_data);
				}
			}
			if !arg.mounts.is_empty() {
				return Err(tg::error!(
					"cannot tg::Process::build() with path mounts. Try tg::Process::run()"
				));
			}

			// Check current command for data to inherit.
			let current_command = state.as_ref().map(|state| state.command.clone());
			let mut current_command_mounts = vec![];
			let mut current_command_env = None;
			let mut current_command_host = None;
			let mut current_command_stdin = None;
			if let Some(command) = current_command {
				let object = command.object(handle).await?;
				current_command_mounts = object.mounts.clone();
				let command_env = object.env.clone();
				if !command_env.is_empty() {
					current_command_env = Some(command_env.clone());
				}
				current_command_host = Some(object.host.clone());
				current_command_stdin = Some(object.stdin.clone());
			}

			// Construct the command combining the arg and the current command.
			let mut command_builder = if let Some(id) = &arg.command {
				let command = tg::Command::with_id(id.clone());
				let object = command.object(handle).await?;
				tg::command::Builder::with_object(&object)
			} else {
				tg::command::Builder::new(
					current_command_host
						.clone()
						.unwrap_or_else(|| host().to_string()),
				)
			};
			command_builder = command_builder.mounts(current_command_mounts);
			if let Some(env) = current_command_env {
				command_builder = command_builder.env(env);
			}
			if let Some(host) = current_command_host {
				command_builder = command_builder.host(host);
			}
			if let Some(stdin) = current_command_stdin {
				command_builder = command_builder.stdin(stdin);
			}
			let command = command_builder.build();
			let command_id = command.id(handle).await?;

			// Finish constructing arg.
			let checksum = arg.checksum;
			let network = if arg.network {
				arg.network
			} else {
				state.as_ref().is_some_and(|state| state.network)
			};
			if network && checksum.is_none() {
				return Err(tg::error!(
					"checksum is required to build a command with network: true"
				));
			}
			tg::process::spawn::Arg {
				checksum,
				command: Some(command_id),
				create: true,
				mounts: vec![],
				network,
				parent: None,
				remote: None,
				retry: false,
				stderr: None,
				stdin: None,
				stdout: None,
			}
		} else {
			arg
		};
		let process = Self::spawn(handle, arg).await?;
		let output = process.wait(handle).await?;
		if output.status != tg::process::Status::Finished {
			let error = output.error.unwrap_or_else(|| {
				tg::error!(
					%process = process.id(),
					"the process failed",
				)
			});
			return Err(error);
		}
		if let Some(exit) = output.exit {
			if let tg::process::Exit::Code { code } = exit {
				if code != 0 {
					return Err(tg::error!("process exited with non-0 exit code: {exit:?}"));
				}
			}
		}
		if let Some(error) = output.error {
			return Err(tg::error!(!error, "process exited with errors"));
		}
		let output = output
			.output
			.ok_or_else(|| tg::error!(%process = process.id(), "expected the output to be set"))?;
		Ok(output)
	}

	pub async fn run<H>(handle: &H, arg: tg::process::spawn::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let arg = if let Some(current) = Self::current(handle).await? {
			// Check current process for mounts, merge with arg mounts.
			let state = current.state().read().unwrap().clone();
			let current_mounts = state.as_ref().map(|state| state.mounts.clone());
			let mut mount_data = vec![];
			if let Some(mounts) = &current_mounts {
				mount_data =
					futures::future::join_all(mounts.iter().map(|mount| mount.data(handle)))
						.await
						.into_iter()
						.collect::<Result<Vec<_>, _>>()?;
			}
			mount_data.extend(arg.mounts);

			// Check current command for data to inherit.
			let current_command = state.as_ref().map(|state| state.command.clone());
			let mut current_command_mounts = vec![];
			let mut current_command_env = None;
			let mut current_command_host = None;
			let mut current_command_stdin = None;
			if let Some(command) = current_command {
				let object = command.object(handle).await?;
				current_command_mounts = object.mounts.clone();
				let command_env = object.env.clone();
				if !command_env.is_empty() {
					current_command_env = Some(command_env.clone());
				}
				current_command_host = Some(object.host.clone());
				current_command_stdin = Some(object.stdin.clone());
			}

			// Infer stdin.
			let mut process_stdin = state.as_ref().and_then(|state| state.stdin.clone());
			if let Some(stdin) = &arg.stdin {
				process_stdin = Some(stdin.clone());
				current_command_stdin = None;
			}

			// Infer stdout and stderr.
			let stderr = if let Some(stderr) = &arg.stderr {
				Some(stderr.clone())
			} else {
				state.as_ref().and_then(|state| state.stderr.clone())
			};
			let stdout = if let Some(stdout) = &arg.stdout {
				Some(stdout.clone())
			} else {
				state.as_ref().and_then(|state| state.stdout.clone())
			};

			// Construct the command combining the arg and the current command.
			let mut command_builder = if let Some(id) = &arg.command {
				let command = tg::Command::with_id(id.clone());
				let object = command.object(handle).await?;
				tg::command::Builder::with_object(&object)
			} else {
				tg::command::Builder::new(
					current_command_host
						.clone()
						.unwrap_or_else(|| host().to_string()),
				)
			};
			command_builder = command_builder.mounts(current_command_mounts);
			if let Some(env) = current_command_env {
				command_builder = command_builder.env(env);
			}
			if let Some(host) = current_command_host {
				command_builder = command_builder.host(host);
			}
			if let Some(stdin) = current_command_stdin {
				command_builder = command_builder.stdin(stdin);
			}
			let command = command_builder.build();
			let command_id = command.id(handle).await?;

			// Finish constructing arg.
			let checksum = arg.checksum;
			let network = if arg.network {
				arg.network
			} else {
				state.as_ref().is_some_and(|state| state.network)
			};
			tg::process::spawn::Arg {
				checksum,
				command: Some(command_id),
				create: true,
				mounts: mount_data,
				network,
				parent: None,
				remote: None,
				retry: false,
				stderr,
				stdin: process_stdin,
				stdout,
			}
		} else {
			arg
		};

		let process = Self::spawn(handle, arg).await?;
		let output = process.wait(handle).await?;
		if output.status != tg::process::Status::Finished {
			let error = output.error.unwrap_or_else(|| {
				tg::error!(
					%process = process.id(),
					"the process failed",
				)
			});
			return Err(error);
		}
		if let Some(exit) = output.exit {
			if let tg::process::Exit::Code { code } = exit {
				if code != 0 {
					return Err(tg::error!("process exited with non-0 exit code: {exit:?}"));
				}
			}
		}
		if let Some(error) = output.error {
			return Err(tg::error!(!error, "process exited with errors"));
		}
		let output = output
			.output
			.ok_or_else(|| tg::error!(%process = process.id(), "expected the output to be set"))?;
		Ok(output)
	}
}

impl Deref for Process {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
