use {
	crate::prelude::*,
	std::{io::IsTerminal as _, path::PathBuf},
};

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub args: tg::value::Array,
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub cwd: Option<PathBuf>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: Option<String>,
	pub mounts: Option<Vec<tg::Either<tg::process::Mount, tg::command::Mount>>>,
	pub name: Option<String>,
	pub network: Option<bool>,
	pub parent: Option<tg::process::Id>,
	pub progress: bool,
	pub remote: Option<String>,
	pub retry: bool,
	pub sandbox: Option<bool>,
	pub stderr: tg::run::Stdio,
	pub stdin: tg::run::Stdio,
	pub stdout: tg::run::Stdio,
	pub user: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub enum Stdio {
	Blob(tg::Blob),
	#[default]
	Inherit,
	Log,
	Null,
	Pipe,
	Pty,
}

pub async fn run<H>(handle: &H, arg: tg::run::Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	let sandbox = arg.sandbox.unwrap_or(false);
	let inherit = !sandbox;
	let (state, command) = if inherit {
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
		(state, command)
	} else {
		(None, None)
	};
	let host = arg
		.host
		.ok_or_else(|| tg::error!("expected the host to be set"))?;
	let executable = arg
		.executable
		.ok_or_else(|| tg::error!("expected the executable to be set"))?;
	let mut builder = tg::Command::builder(host, executable);
	builder = builder.args(arg.args);
	let cwd = if inherit {
		if let Some(command) = &command {
			if command.cwd.is_some() {
				let cwd = std::env::current_dir()
					.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
				Some(cwd)
			} else {
				None
			}
		} else {
			let cwd = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
			Some(cwd)
		}
	} else {
		None
	};
	let cwd = arg.cwd.or(cwd);
	builder = builder.cwd(cwd);
	let env = if inherit {
		let mut env = if let Some(command) = &command {
			command.env.clone()
		} else {
			std::env::vars()
				.map(|(key, value)| (key, value.into()))
				.collect()
		};
		env.remove("TANGRAM_OUTPUT");
		env.remove("TANGRAM_PROCESS");
		env.remove("TANGRAM_URL");
		env
	} else {
		arg.env
	};
	builder = builder.env(env);
	let mut command_mounts = vec![];
	let mut process_mounts = vec![];
	if let Some(mounts) = arg.mounts {
		for mount in mounts {
			match mount {
				tg::Either::Left(mount) => process_mounts.push(mount.to_data()),
				tg::Either::Right(mount) => command_mounts.push(mount),
			}
		}
	} else if inherit {
		if let Some(mounts) = command.as_ref().map(|command| command.mounts.clone()) {
			command_mounts = mounts;
		}
		if let Some(mounts) = state.as_ref().map(|state| state.mounts.clone()) {
			process_mounts = mounts.iter().map(tg::process::Mount::to_data).collect();
		}
	}
	builder = builder.mounts(command_mounts);
	let stdin_blob = if inherit {
		match &arg.stdin {
			tg::run::Stdio::Inherit => command
				.as_ref()
				.map(|command| command.stdin.clone())
				.unwrap_or_default(),
			tg::run::Stdio::Blob(blob) => Some(blob.clone()),
			_ => None,
		}
	} else {
		match &arg.stdin {
			tg::run::Stdio::Blob(blob) => Some(blob.clone()),
			_ => None,
		}
	};
	builder = builder.stdin(stdin_blob);
	if inherit {
		if let Some(Some(user)) = command.as_ref().map(|command| command.user.clone()) {
			builder = builder.user(user);
		}
	} else if let Some(user) = arg.user {
		builder = builder.user(Some(user));
	}
	let command = builder.build();
	let command_id = command.store(handle).await?;
	let mut command = tg::Referent::with_item(command_id);
	if let Some(name) = arg.name {
		command.options.name.replace(name);
	}
	let checksum = arg.checksum;
	let network = if inherit {
		arg.network
			.or(state.as_ref().map(|state| state.network))
			.unwrap_or_default()
	} else {
		arg.network.unwrap_or_default()
	};
	let stderr = if inherit {
		match arg.stderr {
			tg::run::Stdio::Inherit => state.as_ref().map(|state| state.stderr),
			stderr => stderr
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stderr stdio"))?,
		}
	} else {
		match arg.stderr {
			tg::run::Stdio::Inherit => None,
			stderr => stderr
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stderr stdio"))?,
		}
	};
	let stdin = if inherit {
		match arg.stdin {
			tg::run::Stdio::Inherit => state.as_ref().map(|state| state.stdin),
			tg::run::Stdio::Blob(_) => None,
			stdin => stdin
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stdin stdio"))?,
		}
	} else {
		match arg.stdin {
			tg::run::Stdio::Blob(_) | tg::run::Stdio::Inherit => None,
			stdin => stdin
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stdin stdio"))?,
		}
	};
	let stdout = if inherit {
		match arg.stdout {
			tg::run::Stdio::Inherit => state.as_ref().map(|state| state.stdout),
			stdout => stdout
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stdout stdio"))?,
		}
	} else {
		match arg.stdout {
			tg::run::Stdio::Inherit => None,
			stdout => stdout
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stdout stdio"))?,
		}
	};
	if network && checksum.is_none() {
		return Err(tg::error!(
			"a checksum is required to build with network enabled"
		));
	}
	let progress = arg.progress;
	let arg = tg::process::spawn::Arg {
		cached: arg.cached,
		checksum,
		command,
		local: None,
		mounts: process_mounts,
		network,
		parent: arg.parent,
		pty: None,
		remotes: arg.remote.map(|r| vec![r]),
		retry: arg.retry,
		stderr: stderr.unwrap_or_default(),
		stdin: stdin.unwrap_or_default(),
		stdout: stdout.unwrap_or_default(),
	};
	let process = tg::Process::spawn_with_progress(handle, arg, |stream| {
		let writer = std::io::stderr();
		let is_tty = progress && writer.is_terminal();
		tg::progress::write_progress_stream(handle, stream, writer, is_tty)
	})
	.await
	.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;
	let output = process
		.output(handle)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the process output"))?;
	Ok(output)
}

impl Stdio {
	pub fn into_process_stdio(self) -> tg::Result<Option<tg::process::Stdio>> {
		match self {
			Self::Blob(_) => Err(tg::error!("expected a stdio mode")),
			Self::Inherit => Ok(None),
			Self::Log => Ok(Some(tg::process::Stdio::Log)),
			Self::Null => Ok(Some(tg::process::Stdio::Null)),
			Self::Pipe => Ok(Some(tg::process::Stdio::Pipe)),
			Self::Pty => Ok(Some(tg::process::Stdio::Pty)),
		}
	}

	#[must_use]
	pub fn into_stdin(self) -> (Option<tg::process::Stdio>, Option<tg::Blob>) {
		match self {
			Self::Blob(blob) => (None, Some(blob)),
			Self::Inherit => (None, None),
			Self::Log => (Some(tg::process::Stdio::Log), None),
			Self::Null => (Some(tg::process::Stdio::Null), None),
			Self::Pipe => (Some(tg::process::Stdio::Pipe), None),
			Self::Pty => (Some(tg::process::Stdio::Pty), None),
		}
	}
}

impl std::fmt::Display for Stdio {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Blob(blob) => write!(f, "{}", blob.id()),
			Self::Inherit => write!(f, "inherit"),
			Self::Log => write!(f, "log"),
			Self::Null => write!(f, "null"),
			Self::Pipe => write!(f, "pipe"),
			Self::Pty => write!(f, "pty"),
		}
	}
}

impl std::str::FromStr for Stdio {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"inherit" => Ok(Self::Inherit),
			"log" => Ok(Self::Log),
			"null" => Ok(Self::Null),
			"pipe" => Ok(Self::Pipe),
			"pty" => Ok(Self::Pty),
			_ => s
				.parse::<tg::blob::Id>()
				.map(tg::Blob::with_id)
				.map(Self::Blob)
				.map_err(|_| tg::error!(string = %s, "invalid stdio: {s:?}")),
		}
	}
}
