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

pub async fn run<H>(handle: &H, arg: tg::run::Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	let state = if let Some(process) = tg::Process::current()? {
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
		env.insert(key, value.into());
	}
	for (key, value) in arg.env.clone() {
		if let Ok(mutation) = value.try_unwrap_mutation_ref() {
			mutation.apply(&mut env, &key)?;
		} else {
			env.insert(key, value);
		}
	}
	builder = builder.env(env);
	let mut command_mounts = vec![];
	let mut process_mounts = vec![];
	if let Some(mounts) = arg.mounts {
		for mount in mounts {
			match mount {
				Either::Left(mount) => process_mounts.push(mount.data()),
				Either::Right(mount) => command_mounts.push(mount),
			}
		}
	} else {
		if let Some(mounts) = command.as_ref().and_then(|command| command.mounts.clone()) {
			command_mounts = mounts;
		}
		if let Some(mounts) = state.as_ref().map(|state| state.mounts.clone()) {
			process_mounts = mounts.iter().map(tg::process::Mount::data).collect();
		}
	}
	builder = builder.mounts(command_mounts);
	let stdin = if arg.stdin.is_none() {
		command
			.as_ref()
			.map(|command| command.stdin.clone())
			.unwrap_or_default()
	} else if let Some(Some(Either::Right(blob))) = &arg.stdin {
		Some(blob.clone())
	} else {
		None
	};
	builder = builder.stdin(stdin);
	if let Some(Some(user)) = command.as_ref().map(|command| command.user.clone()) {
		builder = builder.user(user);
	}
	let command = builder.build();
	let command_id = command.id(handle).await?;
	let checksum = arg.checksum;
	let network = arg
		.network
		.or(state.as_ref().map(|state| state.network))
		.unwrap_or_default();
	let stderr = arg
		.stderr
		.unwrap_or_else(|| state.as_ref().and_then(|state| state.stderr.clone()));
	let stdin = arg.stdin.unwrap_or_else(|| {
		state
			.as_ref()
			.and_then(|state| state.stdin.clone().map(Either::Left))
	});
	let stdin = match stdin {
		None => None,
		Some(Either::Left(stdio)) => Some(stdio),
		Some(Either::Right(_)) => return Err(tg::error!("expected stdio")),
	};
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
		mounts: process_mounts,
		network,
		parent: arg.parent,
		remote: arg.remote,
		retry: arg.retry,
		stderr,
		stdin,
		stdout,
	};
	let process = tg::Process::spawn(handle, arg).await?;
	let output = process.output(handle).await?;
	Ok(output)
}
