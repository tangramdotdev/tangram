use crate as tg;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub mounts: Option<Vec<tg::process::Mount>>,
	pub network: Option<bool>,
	pub parent: Option<tg::process::Id>,
	pub remote: Option<String>,
	pub retry: bool,
	pub stderr: Option<Option<tg::process::Stdio>>,
	pub stdin: Option<Option<tg::process::Stdio>>,
	pub stdout: Option<Option<tg::process::Stdio>>,
}

impl tg::Process {
	pub async fn run<H>(
		handle: &H,
		command: &tg::Command,
		arg: tg::process::run::Arg,
	) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let state = if let Some(process) = Self::current()? {
			Some(process.load(handle).await?)
		} else {
			None
		};
		let command = command.object(handle).await?;
		let mut builder = tg::Command::builder(command.host.clone());
		builder = builder.args(command.args.clone());
		let cwd = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
		let cwd = command.cwd.clone().unwrap_or(cwd);
		builder = builder.cwd(cwd);
		let mut env = BTreeMap::new();
		for (key, value) in std::env::vars() {
			tg::mutation::mutate(&mut env, key, value.into())?;
		}
		for (key, value) in command.env.clone() {
			tg::mutation::mutate(&mut env, key, value)?;
		}
		if let Some(current) = state.as_ref().map(|state| state.command.clone()) {
			let object = current.object(handle).await?;
			builder = builder.mounts(object.mounts.clone());
		}
		builder = builder.env(env);
		builder = builder.executable(command.executable.clone());
		builder = builder.host(command.host.clone());
		builder = builder.mounts(command.mounts.clone());
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
		let wait = process.wait(handle).await?;
		if let Some(error) = wait.error {
			return Err(error);
		}
		match wait.exit {
			Some(tg::process::Exit::Code { code }) => {
				if code != 0 {
					return Err(tg::error!("the process exited with code {code}"));
				}
			},
			Some(tg::process::Exit::Signal { signal }) => {
				return Err(tg::error!("the process exited with signal {signal}"));
			},
			_ => (),
		}
		let output = wait
			.output
			.ok_or_else(|| tg::error!(%process = process.id(), "expected the output to be set"))?;
		Ok(output)
	}
}
