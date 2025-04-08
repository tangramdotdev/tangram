use crate as tg;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_either::Either;

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub args: tg::value::Array,
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub cwd: Option<PathBuf>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: Option<String>,
	pub mounts: Option<Vec<Either<tg::process::Mount, tg::command::Mount>>>,
	pub network: Option<bool>,
	pub parent: Option<tg::process::Id>,
	pub remote: Option<String>,
	pub retry: bool,
	pub stderr: Option<Option<tg::process::Stdio>>,
	pub stdin: Option<Option<Either<tg::process::Stdio, tg::Blob>>>,
	pub stdout: Option<Option<tg::process::Stdio>>,
	pub user: Option<String>,
}

impl tg::Process {
	pub async fn run<H>(handle: &H, arg: tg::process::run::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let state = if let Some(process) = Self::current()? {
			Some(process.load(handle).await?)
		} else {
			None
		};
		let command = if let Some(state) = &state {
			Some(state.command.object(handle).await?)
		} else {
			None
		};
		let host = arg
			.host
			.ok_or_else(|| tg::error!("expected the host to be set"))?;
		let executable = arg
			.executable
			.ok_or_else(|| tg::error!("expected the executable to be set"))?;
		let mut builder = tg::Command::builder(host, executable);
		builder = builder.args(arg.args);
		let cwd = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
		let cwd = arg.cwd.unwrap_or(cwd);
		builder = builder.cwd(cwd);
		let mut env = BTreeMap::new();
		for (key, value) in std::env::vars() {
			tg::mutation::mutate(&mut env, key, value.into())?;
		}
		for (key, value) in arg.env.clone() {
			tg::mutation::mutate(&mut env, key, value)?;
		}
		builder = builder.env(env);
		let mounts = command
			.as_ref()
			.map(|command| command.mounts.clone())
			.into_iter()
			.flatten();
		builder = builder.mounts(mounts);
		builder = builder.mounts(arg.mounts.as_ref());
		let stdin = if arg.stdin.is_none() {
			command.stdin.clone()
		} else {
			None
		};
		builder = builder.stdin(stdin);
		builder = builder.user(command.user.clone());
		let command = builder.build();
		let command_id = command.id(handle).await?;
		let checksum = arg.checksum;
		let mounts = arg
			.mounts
			.or_else(|| state.as_ref().map(|state| state.mounts.clone()))
			.unwrap_or_default()
			.into_iter()
			.map(|mount| mount.data())
			.collect();
		let network = arg
			.network
			.or(state.as_ref().map(|state| state.network))
			.unwrap_or_default();
		let stderr = arg
			.stderr
			.unwrap_or_else(|| state.as_ref().and_then(|state| state.stderr.clone()));
		let stdin = arg
			.stdin
			.unwrap_or_else(|| state.as_ref().and_then(|state| state.stdin.clone()));
		let stdout = arg
			.stdout
			.unwrap_or_else(|| state.as_ref().and_then(|state| state.stdout.clone()));
		if network && checksum.is_none() {
			return Err(tg::error!(
				"a checksum is required to build with network enabled"
			));
		}
		let arg = tg::process::spawn::Arg {
			cached: arg.cached,
			checksum,
			command: Some(command_id),
			mounts,
			network,
			parent: arg.parent,
			remote: arg.remote,
			retry: arg.retry,
			stderr,
			stdin,
			stdout,
		};
		let process = Self::spawn(handle, arg).await?;
		let output = process.output(handle).await?;
		Ok(output)
	}
}
