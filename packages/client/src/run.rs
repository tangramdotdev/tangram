use {
	crate::prelude::*,
	std::{
		collections::BTreeMap,
		io::IsTerminal as _,
		path::{Path, PathBuf},
		sync::{LazyLock, Mutex},
	},
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
	pub stderr: Option<Option<tg::process::Stdio>>,
	pub stdin: Option<Option<tg::Either<tg::process::Stdio, tg::Blob>>>,
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
	let cwd = if let Some(command) = &command {
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
	};
	let cwd = arg.cwd.or(cwd);
	builder = builder.cwd(cwd);
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
	} else {
		if let Some(mounts) = command.as_ref().map(|command| command.mounts.clone()) {
			command_mounts = mounts;
		}
		if let Some(mounts) = state.as_ref().map(|state| state.mounts.clone()) {
			process_mounts = mounts.iter().map(tg::process::Mount::to_data).collect();
		}
	}
	builder = builder.mounts(command_mounts);
	let stdin = if arg.stdin.is_none() {
		command
			.as_ref()
			.map(|command| command.stdin.clone())
			.unwrap_or_default()
	} else if let Some(Some(tg::Either::Right(blob))) = &arg.stdin {
		Some(blob.clone())
	} else {
		None
	};
	builder = builder.stdin(stdin);
	if let Some(Some(user)) = command.as_ref().map(|command| command.user.clone()) {
		builder = builder.user(user);
	}
	let command = builder.build();
	let command_id = command.store(handle).await?;
	let mut command = tg::Referent::with_item(command_id);
	if let Some(name) = arg.name {
		command.options.name.replace(name);
	}
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
			.and_then(|state| state.stdin.clone().map(tg::Either::Left))
	});
	let stdin = match stdin {
		None => None,
		Some(tg::Either::Left(stdio)) => Some(stdio),
		Some(tg::Either::Right(_)) => {
			return Err(tg::error!("expected stdio"));
		},
	};
	let stdout = arg
		.stdout
		.unwrap_or_else(|| state.as_ref().and_then(|state| state.stdout.clone()));
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
		remotes: arg.remote.map(|r| vec![r]),
		retry: arg.retry,
		stderr,
		stdin,
		stdout,
	};
	let stream = tg::Process::spawn(handle, arg).await?;
	let writer = std::io::stderr();
	let is_tty = progress && writer.is_terminal();
	let process = tg::progress::write_progress_stream(handle, stream, writer, is_tty)
		.await
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;
	let output = process
		.output(handle)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the process output"))?;
	Ok(output)
}

pub async fn run2<H>(handle: &H, arg: Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	let host = arg
		.host
		.clone()
		.or_else(|| {
			if cfg!(target_os = "linux") {
				#[cfg(target_arch = "aarch64")]
				return Some("aarch64-linux".into());

				#[cfg(target_arch = "x86_64")]
				return Some("x86_64-linux".into());
			}
			if cfg!(target_os = "macos") {
				#[cfg(target_arch = "aarch64")]
				return Some("aarch64-macos".into());

				#[cfg(target_arch = "x86_64")]
				return Some("x86_64-macos".into());
			}
			None
		})
		.ok_or_else(|| tg::error!("unknown host"))?;
	if needs_sandbox(&arg) {
		run_sandboxed(handle, host, arg).await
	} else {
		run_unsandboxed(handle, host, arg).await
	}
}

async fn run_sandboxed<H>(handle: &H, host: String, arg: Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	// TODO: handle signals
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

	// Get the executable.
	let executable = arg
		.executable
		.ok_or_else(|| tg::error!("expected the executable to be set"))?;

	let mut builder = tg::Command::builder(host, executable);
	builder = builder.args(arg.args);
	let cwd = if let Some(command) = &command {
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
	};
	let cwd = arg.cwd.or(cwd);
	builder = builder.cwd(cwd);
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
	} else {
		if let Some(mounts) = command.as_ref().map(|command| command.mounts.clone()) {
			command_mounts = mounts;
		}
		if let Some(mounts) = state.as_ref().map(|state| state.mounts.clone()) {
			process_mounts = mounts.iter().map(tg::process::Mount::to_data).collect();
		}
	}
	builder = builder.mounts(command_mounts);
	let stdin = if arg.stdin.is_none() {
		command
			.as_ref()
			.map(|command| command.stdin.clone())
			.unwrap_or_default()
	} else if let Some(Some(tg::Either::Right(blob))) = &arg.stdin {
		Some(blob.clone())
	} else {
		None
	};
	builder = builder.stdin(stdin);
	if let Some(Some(user)) = command.as_ref().map(|command| command.user.clone()) {
		builder = builder.user(user);
	}
	let command = builder.build();
	let command_id = command.store(handle).await?;
	let mut command = tg::Referent::with_item(command_id);
	if let Some(name) = arg.name {
		command.options.name.replace(name);
	}
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
			.and_then(|state| state.stdin.clone().map(tg::Either::Left))
	});
	let stdin = match stdin {
		None => None,
		Some(tg::Either::Left(stdio)) => Some(stdio),
		Some(tg::Either::Right(_)) => {
			return Err(tg::error!("expected stdio"));
		},
	};
	let stdout = arg
		.stdout
		.unwrap_or_else(|| state.as_ref().and_then(|state| state.stdout.clone()));
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
		remotes: arg.remote.map(|r| vec![r]),
		retry: arg.retry,
		stderr,
		stdin,
		stdout,
	};
	let stream = tg::Process::spawn(handle, arg).await?;
	let writer = std::io::stderr();
	let is_tty = progress && writer.is_terminal();
	let process = tg::progress::write_progress_stream(handle, stream, writer, is_tty)
		.await
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;
	let output = process
		.output(handle)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the process output"))?;
	Ok(output)
}

async fn run_unsandboxed<H>(handle: &H, host: String, arg: Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	// Create the output directory.
	let temp = tempfile::TempDir::new()
		.map_err(|source| tg::error!(!source, "failed to create the temporary directory"))?;
	tokio::fs::create_dir_all(temp.path())
		.await
		.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;
	let output_path = temp.path().join("output");

	// Get the executable.
	let executable = arg
		.executable
		.ok_or_else(|| tg::error!("expected the executable to be set"))?
		.to_data();

	let mut args = Vec::new();
	let mut env = std::env::vars().collect::<BTreeMap<_, _>>();

	env.remove("TANGRAM_OUTPUT");
	env.remove("TANGRAM_PROCESS");
	env.remove("TANGRAM_URL");

	// Render the executable.
	let executable = match host.as_str() {
		"builtin" => {
			let exe = tangram_executable_path();
			args.insert(0, "builtin".to_owned());
			args.insert(1, executable.to_string());
			exe
		},

		"js" => {
			let exe = tangram_executable_path();
			args.insert(0, "js".to_owned());
			args.insert(1, executable.to_string());
			exe
		},

		_ => match executable {
			tg::command::data::Executable::Artifact(executable) => {
				let mut path = tg::checkout(handle, tg::checkout::Arg {
					artifact: executable.artifact.clone(),
					dependencies: true,
					extension: None,
					force: false,
					lock: None,
					path: None,
				}).await.map_err(|source| tg::error!(!source, executable = %executable.artifact, "failed to check out the artifact"))?;
				if let Some(executable_path) = &executable.path {
					path.push(executable_path);
				}
				path
			},
			tg::command::data::Executable::Module(_) => {
				return Err(tg::error!("invalid executable"));
			},
			tg::command::data::Executable::Path(executable) => executable.path.clone(),
		},
	};

	// Render the args and env.
	args.extend(render_args(&arg.args, &output_path)?);
	env.extend(render_env(&arg.env, &output_path)?);

	// Run the process.
	let result = tokio::process::Command::new(executable)
		.args(args)
		.envs(env)
		.env("TANGRAM_OUTPUT", &output_path)
		.stdin(std::process::Stdio::inherit())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit())
		.output()
		.await
		.map_err(|source| tg::error!(!source, "failed to run the process"))?;

	if !result.status.success() {
		return Err(tg::error!("the process failed"));
	}

	// Check in the output if it exists.
	let exists = tokio::fs::try_exists(&output_path)
		.await
		.map_err(|source| tg::error!(!source, "failed to check if the output path exists"))?;
	if exists {
		let arg = tg::checkin::Arg {
			options: tg::checkin::Options {
				destructive: true,
				deterministic: true,
				ignore: false,
				lock: None,
				locked: true,
				root: true,
				..Default::default()
			},
			path: output_path,
			updates: Vec::new(),
		};
		let artifact = tg::checkin::checkin(handle, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
		Ok(artifact.into())
	} else {
		Ok(tg::Value::Null)
	}
}

fn needs_sandbox(arg: &Arg) -> bool {
	if arg.cached.is_some_and(|cached| cached) {
		return true;
	}
	if arg.checksum.is_some() {
		return true;
	}
	if arg.mounts.as_ref().is_some_and(|m| !m.is_empty()) {
		return true;
	}
	if arg.network.is_some_and(|network| !network) {
		return true;
	}
	if arg.remote.is_some() {
		return true;
	}
	false
}

pub fn render_args(args: &[tg::Value], output_path: &Path) -> tg::Result<Vec<String>> {
	args.iter()
		.map(|value| render_value(value, output_path))
		.collect::<tg::Result<Vec<_>>>()
}

pub fn render_env(
	env: &tg::value::Map,
	output_path: &Path,
) -> tg::Result<BTreeMap<String, String>> {
	let mut output = tg::value::Map::new();
	for (key, value) in env {
		let mutation = match value {
			tg::Value::Mutation(value) => value.clone(),
			value => tg::Mutation::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(&mut output, key)?;
	}
	let output = output
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = render_value(value, output_path)?;
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<_>>()?;
	Ok(output)
}

pub fn render_value(value: &tg::Value, output_path: &Path) -> tg::Result<String> {
	let artifacts_path = artifacts_path();
	match value {
		tg::Value::String(string) => Ok(string.clone()),
		tg::Value::Template(template) => template.try_render_sync(|component| match component {
			tg::template::Component::String(string) => Ok(string.clone().into()),
			tg::template::Component::Artifact(artifact) => Ok(artifacts_path
				.join(artifact.id().to_string())
				.to_str()
				.unwrap()
				.to_owned()
				.into()),
			tg::template::Component::Placeholder(placeholder) => {
				if placeholder.name == "output" {
					Ok(output_path.to_str().unwrap().to_owned().into())
				} else {
					Err(tg::error!(
						name = %placeholder.name,
						"invalid placeholder"
					))
				}
			},
		}),
		tg::Value::Placeholder(placeholder) => {
			if placeholder.name == "output" {
				Ok(output_path.to_str().unwrap().to_owned())
			} else {
				Err(tg::error!(
					name = %placeholder.name,
					"invalid placeholder"
				))
			}
		},
		_ => Ok(value.to_string()),
	}
}

pub static CLOSEST_ARTIFACT_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
	let mut closest_artifact_path = None;
	let cwd = tangram_util::env::current_exe()
		.expect("Failed to get the current directory")
		.canonicalize()
		.expect("failed to canonicalize current directory");
	for path in cwd.ancestors().skip(1) {
		let directory = path.join(".tangram/artifacts");
		if directory.exists() {
			closest_artifact_path = Some(directory);
			break;
		}
	}
	if closest_artifact_path.is_none() {
		let opt_path = std::path::Path::new("/opt/tangram/artifacts");
		if opt_path.exists() {
			closest_artifact_path.replace(opt_path.to_owned());
		}
	}
	closest_artifact_path.expect("Failed to find the closest artifact path")
});

pub static TANGRAM_EXECUTABLE_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);
pub static TANGRAM_ARTIFACTS_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);

pub fn set_artifacts_path(p: impl AsRef<PathBuf>) {
	TANGRAM_ARTIFACTS_PATH
		.lock()
		.unwrap()
		.replace(p.as_ref().to_owned());
}

pub fn artifacts_path() -> PathBuf {
	TANGRAM_ARTIFACTS_PATH
		.lock()
		.unwrap()
		.clone()
		.unwrap_or_else(|| CLOSEST_ARTIFACT_PATH.clone())
}

pub fn set_tangram_executable_path(p: impl AsRef<Path>) {
	TANGRAM_EXECUTABLE_PATH
		.lock()
		.unwrap()
		.replace(p.as_ref().to_owned());
}

pub fn tangram_executable_path() -> PathBuf {
	TANGRAM_EXECUTABLE_PATH
		.lock()
		.unwrap()
		.clone()
		.unwrap_or_else(|| "tangram".into())
}
