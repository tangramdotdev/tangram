use {
	super::State,
	futures::{StreamExt as _, TryStreamExt as _, future},
	std::{collections::BTreeMap, rc::Rc},
	tangram_client::prelude::*,
	tangram_v8::Serde,
};

/// A child process stored in state, awaiting wait().
pub(crate) struct ChildProcess {
	pub child: tokio::process::Child,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct SpawnOutput {
	process: tg::Either<tg::process::Id, i32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	token: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	wait: Option<WaitOutput>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct WaitOutput {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

	exit: u8,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	output: Option<tg::value::Data>,

	// TODO: Handle stdout/stderr for unsandboxed processes.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	stdout: Option<Vec<u8>>,

	// TODO: Handle stderr for unsandboxed processes.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	stderr: Option<Vec<u8>>,
}

pub async fn get(
	state: Rc<State>,
	args: (Serde<tg::process::Id>, Option<String>),
) -> tg::Result<Serde<tg::process::Data>> {
	let (Serde(id), _) = args;
	let handle = state.handle.clone();
	let data = state
		.main_runtime_handle
		.spawn(async move {
			let tg::process::get::Output { data, .. } = handle.get_process(&id).await?;
			Ok::<_, tg::Error>(data)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to get the process"))?;
	Ok(Serde(data))
}

pub async fn spawn(
	state: Rc<State>,
	args: (Serde<tg::process::spawn::Arg>,),
) -> tg::Result<Serde<SpawnOutput>> {
	let (Serde(arg),) = args;
	if needs_sandbox(&arg) {
		spawn_sandboxed(state, arg).await
	} else {
		spawn_unsandboxed(state, arg).await
	}
}

async fn spawn_sandboxed(
	state: Rc<State>,
	arg: tg::process::spawn::Arg,
) -> tg::Result<Serde<SpawnOutput>> {
	let stream = state
		.main_runtime_handle
		.spawn({
			let handle = state.handle.clone();
			async move {
				let stream = handle
					.try_spawn_process(arg)
					.await?
					.and_then(|event| {
						let result =
							event.try_map_output(|output: Option<tg::process::spawn::Output>| {
								output.ok_or_else(|| tg::error!("expected a process"))
							});
						future::ready(result)
					})
					.boxed();
				Ok::<_, tg::Error>(stream)
			}
		})
		.await
		.unwrap()?;
	let writer = super::log::Writer::new(state.clone(), tg::process::log::Stream::Stderr);
	let handle = state.handle.clone();
	let output = tg::progress::write_progress_stream(&handle, stream, writer, false).await?;
	let spawn_output = SpawnOutput {
		process: tg::Either::Left(
			output
				.process
				.ok_or_else(|| tg::error!("expected a process ID"))?,
		),
		remote: output.remote,
		token: output.token,
		wait: output.wait.map(|w| WaitOutput {
			error: w.error,
			exit: w.exit,
			output: w.output,
			stdout: None,
			stderr: None,
		}),
	};
	Ok(Serde(spawn_output))
}

async fn spawn_unsandboxed(
	state: Rc<State>,
	arg: tg::process::spawn::Arg,
) -> tg::Result<Serde<SpawnOutput>> {
	// Get the command data.
	let command_id = arg.command.item.clone();
	let handle = state.handle.clone();
	let command_data = state
		.main_runtime_handle
		.spawn(async move {
			let command = tg::Command::with_id(command_id);
			command.data(&handle).await
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to get the command data"))?;

	// Render the executable.
	let host = command_data.host.clone();
	let mut args = Vec::new();
	let mut env = std::env::vars().collect::<BTreeMap<_, _>>();

	env.remove("TANGRAM_OUTPUT");
	env.remove("TANGRAM_PROCESS");
	env.remove("TANGRAM_URL");

	let executable = match host.as_str() {
		"builtin" => {
			let tg_exe = tangram_util::env::current_exe()
				.map_err(|source| tg::error!(!source, "failed to get the current executable"))?;
			args.insert(0, "builtin".to_owned());
			args.insert(1, command_data.executable.to_string());
			tg_exe
		},

		"js" => {
			let tg_exe = tangram_util::env::current_exe()
				.map_err(|source| tg::error!(!source, "failed to get the current executable"))?;
			args.insert(0, "js".to_owned());
			args.insert(1, command_data.executable.to_string());
			tg_exe
		},

		_ => match &command_data.executable {
			tg::command::data::Executable::Artifact(executable) => {
				let mut path = tg::run::CLOSEST_ARTIFACT_PATH.join(executable.artifact.to_string());
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

	// Create a temporary output directory.
	let temp = tempfile::TempDir::new()
		.map_err(|source| tg::error!(!source, "failed to create a temp directory"))?;
	let output_path = temp.path().join("output");

	// Convert data args and env to values for rendering.
	let value_args: Vec<tg::Value> = command_data
		.args
		.into_iter()
		.map(tg::Value::try_from_data)
		.collect::<tg::Result<_>>()?;
	let value_env: tg::value::Map = command_data
		.env
		.into_iter()
		.map(|(k, v)| Ok::<_, tg::Error>((k, tg::Value::try_from_data(v)?)))
		.collect::<tg::Result<_>>()?;

	// Render the args and env.
	args.extend(tg::run::render_args(&value_args, &output_path)?);
	env.extend(tg::run::render_env(&value_env, &output_path)?);

	// Spawn the process with piped stdout and stderr.
	let child = tokio::process::Command::new(executable)
		.args(args)
		.envs(env)
		.stdin(std::process::Stdio::inherit())
		.stdout(std::process::Stdio::piped())
		.stderr(std::process::Stdio::piped())
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

	// Get the PID.
	let pid = child
		.id()
		.ok_or_else(|| tg::error!("failed to get the child process ID"))? as i32;

	// Store the child process in state.
	state
		.children
		.borrow_mut()
		.insert(pid, ChildProcess { child });

	Ok(Serde(SpawnOutput {
		process: tg::Either::Right(pid),
		remote: None,
		token: None,
		wait: None,
	}))
}

pub async fn wait(
	state: Rc<State>,
	args: (
		Serde<tg::Either<tg::process::Id, i32>>,
		Serde<tg::process::wait::Arg>,
	),
) -> tg::Result<Serde<WaitOutput>> {
	let (Serde(id), Serde(arg)) = args;
	match id {
		tg::Either::Left(id) => wait_sandboxed(state, id, arg).await,
		tg::Either::Right(pid) => wait_unsandboxed(state, pid).await,
	}
}

async fn wait_sandboxed(
	state: Rc<State>,
	id: tg::process::Id,
	arg: tg::process::wait::Arg,
) -> tg::Result<Serde<WaitOutput>> {
	let handle = state.handle.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let output = handle.wait_process(&id, arg).await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(Serde(WaitOutput {
		error: output.error,
		exit: output.exit,
		output: output.output,
		stdout: None,
		stderr: None,
	}))
}

async fn wait_unsandboxed(state: Rc<State>, pid: i32) -> tg::Result<Serde<WaitOutput>> {
	// Remove the child process from state.
	let mut child = state
		.children
		.borrow_mut()
		.remove(&pid)
		.ok_or_else(|| tg::error!(%pid, "unknown child process"))?
		.child;

	// Wait for the child process to complete.
	let output = child
		.wait_with_output()
		.await
		.map_err(|source| tg::error!(!source, "failed to wait for the child process"))?;

	// Get the exit code.
	let exit = output.status.code().map(|c| c as u8).unwrap_or(128);

	Ok(Serde(WaitOutput {
		error: None,
		exit,
		output: None,
		stdout: Some(output.stdout),
		stderr: Some(output.stderr),
	}))
}

fn needs_sandbox(arg: &tg::process::spawn::Arg) -> bool {
	if arg.cached.is_some_and(|cached| cached) {
		return true;
	}
	if arg.checksum.is_some() {
		return true;
	}
	if !arg.mounts.is_empty() {
		return true;
	}
	if !arg.network {
		return true;
	}
	if arg
		.remotes
		.as_ref()
		.is_some_and(|remotes| !remotes.is_empty())
	{
		return true;
	}
	false
}
