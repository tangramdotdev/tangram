use std::{collections::BTreeMap, os::unix::process::ExitStatusExt};

use num::ToPrimitive;
use tangram_client::prelude::*;
use tempfile::TempDir;

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct WaitOutput {
	pub exit: u8,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdout: Option<Vec<u8>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stderr: Option<Vec<u8>>,
}

pub(crate) struct ChildProcess {
	pub(crate) child: tokio::process::Child,
	temp: TempDir,
}

pub(crate) async fn spawn_unsandboxed<H>(
	handle: &H,
	arg: tg::process::spawn::Arg,
) -> tg::Result<ChildProcess>
where
	H: tg::Handle,
{
	// Get the command data.
	let command_id = arg.command.item.clone();
	let command = tg::Command::with_id(command_id);
	let command = command
		.data(handle)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the command data"))?;

	// Render the executable.
	let host = command.host.clone();
	let mut args = Vec::new();
	let mut env = std::env::vars().collect::<BTreeMap<_, _>>();

	let executable = match host.as_str() {
		"builtin" => {
			let exe = tg::run::tangram_executable_path();
			args.insert(0, "builtin".to_owned());
			args.insert(1, command.executable.to_string());
			exe
		},

		"js" => {
			let exe = tg::run::tangram_executable_path();
			args.insert(0, "js".to_owned());
			args.insert(1, command.executable.to_string());
			exe
		},

		_ => match &command.executable {
			tg::command::data::Executable::Artifact(executable) => {
				let mut path = tg::checkout(handle,tg::checkout::Arg {
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

	// Create a temp for the output.
	let temp = tempfile::tempdir()
		.map_err(|source| tg::error!(!source, "failed to create the temporary directory"))?;
	tokio::fs::create_dir_all(temp.path())
		.await
		.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;
	let output_path = temp.path().join("output");

	// Convert data.
	let args_: Vec<tg::Value> = command
		.args
		.into_iter()
		.map(tg::Value::try_from_data)
		.collect::<tg::Result<_>>()?;
	let env_: tg::value::Map = command
		.env
		.into_iter()
		.map(|(k, v)| Ok::<_, tg::Error>((k, tg::Value::try_from_data(v)?)))
		.collect::<tg::Result<_>>()?;

	// Render the args and env.
	args.extend(tg::run::render_args(&args_, &output_path)?);
	env.extend(tg::run::render_env(&env_, &output_path)?);

	// Spawn the process with piped stdout and stderr.
	let child = tokio::process::Command::new(executable)
		.args(args)
		.envs(env)
		.env("TANGRAM_OUTPUT", &output_path)
		.stdin(std::process::Stdio::inherit())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit())
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

	Ok(ChildProcess { child, temp })
}

pub(crate) async fn wait_unsandboxed<H>(handle: &H, child: ChildProcess) -> tg::Result<WaitOutput>
where
	H: tg::Handle,
{
	let output = child
		.child
		.wait_with_output()
		.await
		.map_err(|source| tg::error!(!source, "failed to wait for the output"))?;
	let status = output.status;
	let exit = None
		.or(status.code())
		.or(status.signal().map(|signal| 128 + signal))
		.unwrap()
		.to_u8()
		.unwrap();
	let stdout = output.stdout;
	let stderr = output.stderr;
	let output_path = child.temp.path().join("output");
	let mut error = None;
	let mut output = None;
	if matches!(tokio::fs::try_exists(&output_path).await, Ok(true)) {
		let result = tg::checkin(
			handle,
			tg::checkin::Arg {
				path: output_path,
				options: tg::checkin::Options {
					destructive: true,
					deterministic: true,
					ignore: false,
					lock: None,
					locked: true,
					root: true,
					..Default::default()
				},
				updates: Vec::new(),
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to check in the output"));
		match result {
			Ok(artifact) => {
				output.replace(tg::value::Data::Object(artifact.id().into()));
			},
			Err(e) => {
				let e = e.to_data_or_id();
				error.replace(e);
			},
		}
	}
	let output = WaitOutput {
		exit,
		output,
		error,
		stdout: (!stdout.is_empty()).then_some(stdout),
		stderr: (!stderr.is_empty()).then_some(stderr),
	};
	Ok(output)
}
