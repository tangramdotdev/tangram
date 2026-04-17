use {
	crate::prelude::*,
	futures::{
		Stream, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	serde_with::serde_as,
	std::{
		collections::{BTreeMap, BTreeSet},
		future::Future,
		io::IsTerminal as _,
		os::unix::process::ExitStatusExt as _,
		path::{Path, PathBuf},
		sync::{Arc, Mutex, RwLock},
	},
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_default, is_false},
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cached: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::Checksum>,

	pub command: tg::Referent<tg::command::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[serde(alias = "remote", default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<tg::Either<tg::sandbox::create::Arg, tg::sandbox::Id>>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub stderr: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	pub stdin: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	pub stdout: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tty: Option<tg::Either<bool, tg::process::Tty>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	pub process: tg::process::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub wait: Option<tg::process::wait::Output>,
}

impl<O: 'static> tg::Process<O> {
	pub async fn spawn(arg: tg::process::spawn::Arg) -> tg::Result<tg::Process<O>>
	where
		O: 'static,
	{
		let handle = tg::handle()?;
		Self::spawn_with_handle(handle, arg).await
	}

	pub async fn spawn_with_handle<H>(
		handle: &H,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<tg::Process<O>>
	where
		H: tg::Handle,
	{
		Self::spawn_with_progress_with_handle(handle, arg, |stream| async move {
			stream
				.try_last()
				.await?
				.and_then(|event| event.try_unwrap_output().ok())
				.ok_or_else(|| tg::error!("expected the stream to contain an output event"))
		})
		.await
	}

	pub async fn spawn_with_progress<F, Fut, T>(
		arg: tg::process::spawn::Arg,
		progress: F,
	) -> tg::Result<T>
	where
		F: FnOnce(BoxStream<'static, tg::Result<tg::progress::Event<tg::Process<O>>>>) -> Fut,
		Fut: Future<Output = tg::Result<T>>,
	{
		let handle = tg::handle()?;
		Self::spawn_with_progress_with_handle(handle, arg, progress).await
	}

	pub async fn spawn_with_progress_with_handle<H, F, Fut, T>(
		handle: &H,
		mut arg: tg::process::spawn::Arg,
		progress: F,
	) -> tg::Result<T>
	where
		H: tg::Handle,
		F: FnOnce(BoxStream<'static, tg::Result<tg::progress::Event<tg::Process<O>>>>) -> Fut,
		Fut: Future<Output = tg::Result<T>>,
	{
		let handle = handle.clone();
		let sandboxed = arg.sandbox.is_some();
		if !sandboxed {
			let process = Self::spawn_unsandboxed(&handle, arg).await?;
			let stream = stream::once(future::ok(tg::progress::Event::Output(process))).boxed();
			return progress(stream).await;
		}
		let provide_stderr = matches!(
			arg.stderr,
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		);
		let provide_stdin = matches!(
			arg.stdin,
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		);
		let provide_stdout = matches!(
			arg.stdout,
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		);
		let no_tty = matches!(arg.tty, Some(tg::Either::Left(false)));
		let raw = sandboxed && arg.stdin.is_inherit() && !no_tty && std::io::stdin().is_terminal();
		let mut tty = match arg.tty.take() {
			Some(tg::Either::Left(true)) => {
				super::stdio::get_tty_size().map(|size| tg::process::Tty { size })
			},
			Some(tg::Either::Right(tty)) => Some(tty),
			_ => None,
		};
		let stdin = if sandboxed && arg.stdin.is_inherit() {
			let stdin = if raw {
				tg::process::Stdio::Tty
			} else {
				tg::process::Stdio::Pipe
			};
			arg.stdin = stdin.clone();
			Some(stdin)
		} else {
			None
		};
		let stdout = if sandboxed && arg.stdout.is_inherit() {
			let stdout = if !no_tty && std::io::stdout().is_terminal() {
				tg::process::Stdio::Tty
			} else {
				tg::process::Stdio::Pipe
			};
			arg.stdout = stdout.clone();
			Some(stdout)
		} else {
			None
		};
		let stderr = if sandboxed && arg.stderr.is_inherit() {
			let stderr = if !no_tty && std::io::stderr().is_terminal() {
				tg::process::Stdio::Tty
			} else {
				tg::process::Stdio::Pipe
			};
			arg.stderr = stderr.clone();
			Some(stderr)
		} else {
			None
		};
		if tty.is_none()
			&& (stdin.as_ref().is_some_and(tg::process::Stdio::is_tty)
				|| stdout.as_ref().is_some_and(tg::process::Stdio::is_tty)
				|| stderr.as_ref().is_some_and(tg::process::Stdio::is_tty))
		{
			tty = super::stdio::get_tty_size().map(|size| tg::process::Tty { size });
		}
		let tty_ = tty.is_some();
		arg.tty = tty.map(tg::Either::Right);
		if tty_ && (stdin.is_some() || stdout.is_some() || stderr.is_some()) {
			let mut object = tg::Command::with_id(arg.command.item.clone())
				.object_with_handle(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to load the command"))?
				.as_ref()
				.clone();
			let mut changed = false;
			for name in ["COLORTERM", "TERM"] {
				if object.env.contains_key(name) {
					continue;
				}
				let Ok(value) = std::env::var(name) else {
					continue;
				};
				object.env.insert(name.to_owned(), tg::Value::String(value));
				changed = true;
			}
			if changed {
				let id = tg::Command::with_object(object)
					.store_with_handle(&handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to store the command"))?;
				arg.command.item = id;
			}
		}
		let stream = handle
			.spawn_process(arg)
			.await?
			.and_then(move |event| {
				let handle = handle.clone();
				let stdin = stdin.clone();
				let stdout = stdout.clone();
				let stderr = stderr.clone();
				async move {
					let event = match event {
						tg::progress::Event::Output(output) => {
							let wait = output
								.wait
								.map(tg::process::Wait::try_from_data)
								.transpose()?;
							let id = output.process;
							let remote = output.remote.clone();
							let stdio_task = if stdin.is_some()
								|| stdout.is_some() || stderr.is_some()
								|| tty_
							{
								let handle = handle.clone();
								let id = id.clone();
								let remote = remote.clone();
								let stdin = stdin.clone();
								let stdout = stdout.clone();
								let stderr = stderr.clone();
								Some(tangram_futures::task::Shared::spawn(move |_| async move {
									super::stdio::stdio_task(
										handle, id, remote, stdin, stdout, stderr, tty_, raw,
									)
									.await
								}))
							} else {
								None
							};
							let stderr = if provide_stderr {
								super::stdio::Reader::from_process(
									id.clone(),
									remote.clone(),
									tg::process::stdio::Stream::Stderr,
								)
							} else {
								super::stdio::Reader::unavailable(
									tg::process::stdio::Stream::Stderr,
								)
							};
							let stdin = if provide_stdin {
								super::stdio::Writer::from_process(
									id.clone(),
									remote.clone(),
									tg::process::stdio::Stream::Stdin,
								)
							} else {
								super::stdio::Writer::unavailable(tg::process::stdio::Stream::Stdin)
							};
							let stdout = if provide_stdout {
								super::stdio::Reader::from_process(
									id.clone(),
									remote.clone(),
									tg::process::stdio::Stream::Stdout,
								)
							} else {
								super::stdio::Reader::unavailable(
									tg::process::stdio::Stream::Stdout,
								)
							};
							let inner = Arc::new(super::Inner {
								cached: Some(output.cached),
								id,
								metadata: RwLock::new(None),
								remote,
								state: RwLock::new(None),
								stderr,
								stdin,
								stdio_task,
								stdout,
								task: None,
								token: output.token,
								wait: Mutex::new(wait),
								pid: None,
							});
							let process = Self(inner, std::marker::PhantomData);
							tg::progress::Event::Output(process)
						},
						tg::progress::Event::Log(log) => tg::progress::Event::Log(log),
						tg::progress::Event::Diagnostic(diagnostic) => {
							tg::progress::Event::Diagnostic(diagnostic)
						},
						tg::progress::Event::Indicators(indicators) => {
							tg::progress::Event::Indicators(indicators)
						},
					};
					Ok(event)
				}
			})
			.boxed();
		let process = progress(stream).await?;
		Ok(process)
	}

	async fn spawn_unsandboxed<H>(
		handle: &H,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<tg::Process<O>>
	where
		H: tg::Handle,
	{
		if arg.tty.is_some() {
			return Err(tg::error!("tty is not supported for unsandboxed processes"));
		}
		if arg.stdin.is_blob() {
			return Err(tg::error!(
				"blob stdin is not supported for unsandboxed processes"
			));
		}

		let command = tg::Command::with_id(arg.command.item().clone())
			.data_with_handle(handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to load the command"))?;
		if command.stdin.is_some() {
			return Err(tg::error!(
				"command stdin blobs are not supported for unsandboxed processes"
			));
		}
		if command.user.is_some() {
			return Err(tg::error!(
				"setting a user is not supported for unsandboxed processes"
			));
		}

		let id = tg::process::Id::new();
		let temp = tangram_util::fs::Temp::new()
			.map_err(|source| tg::error!(!source, "failed to create a temp directory"))?;
		tokio::fs::create_dir(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create a temp directory"))?;
		let output_path = temp.path().join(id.to_string());
		let artifacts = checkout_artifacts(handle, &command).await?;
		let env = render_env(&command.env, &artifacts, &output_path)?;
		let (executable, args) = render_command(&command, &artifacts, &output_path)?;

		let mut command_ = tokio::process::Command::new(&executable);
		command_.args(&args);
		command_.env_clear();
		command_.envs(&env);
		if let Some(cwd) = &command.cwd {
			command_.current_dir(cwd);
		}
		command_.stdin(convert_stdio(
			&arg.stdin,
			tg::process::stdio::Stream::Stdin,
		)?);
		command_.stdout(convert_stdio(
			&arg.stdout,
			tg::process::stdio::Stream::Stdout,
		)?);
		command_.stderr(convert_stdio(
			&arg.stderr,
			tg::process::stdio::Stream::Stderr,
		)?);

		let mut child = command_.spawn().map_err(|source| {
			tg::error!(
				!source,
				executable = %executable.display(),
				"failed to spawn the process"
			)
		})?;
		let pid = child
			.id()
			.ok_or_else(|| tg::error!("failed to get the process id"))?;
		let stderr = if arg.stderr.is_pipe() {
			child.stderr.take().map_or_else(
				|| super::stdio::Reader::unavailable(tg::process::stdio::Stream::Stderr),
				super::stdio::Reader::from_stderr,
			)
		} else {
			super::stdio::Reader::unavailable(tg::process::stdio::Stream::Stderr)
		};
		let stdin = if arg.stdin.is_pipe() {
			child.stdin.take().map_or_else(
				|| super::stdio::Writer::unavailable(tg::process::stdio::Stream::Stdin),
				super::stdio::Writer::from_stdin,
			)
		} else {
			super::stdio::Writer::unavailable(tg::process::stdio::Stream::Stdin)
		};
		let stdout = if arg.stdout.is_pipe() {
			child.stdout.take().map_or_else(
				|| super::stdio::Reader::unavailable(tg::process::stdio::Stream::Stdout),
				super::stdio::Reader::from_stdout,
			)
		} else {
			super::stdio::Reader::unavailable(tg::process::stdio::Stream::Stdout)
		};

		let mut task = tangram_futures::task::Shared::spawn({
			let handle = handle.clone();
			move |_| async move { Self::wait_unsandboxed(handle, child, output_path, temp).await }
		});
		task.detach();

		let inner = Arc::new(super::Inner {
			cached: Some(false),
			id,
			metadata: RwLock::new(None),
			pid: Some(pid),
			remote: None,
			state: RwLock::new(None),
			stderr,
			stdin,
			stdio_task: None,
			stdout,
			task: Some(task),
			token: None,
			wait: Mutex::new(None),
		});
		let process = Self(inner, std::marker::PhantomData);
		Ok(process)
	}

	async fn wait_unsandboxed<H>(
		handle: H,
		mut child: tokio::process::Child,
		output_path: PathBuf,
		_temp: tangram_util::fs::Temp,
	) -> tg::Result<tg::process::wait::Output>
	where
		H: tg::Handle,
	{
		let status = child
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the process"))?;
		let exit = exit_status_to_code(status)?;
		let mut output = tg::process::wait::Output {
			error: None,
			exit,
			output: None,
		};

		let exists = tokio::fs::try_exists(&output_path)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to determine if the output path exists")
			})?;

		if exists {
			let output_bytes = xattr::get(&output_path, "user.tangram.output")
				.map_err(|source| tg::error!(!source, "failed to read the output xattr"))?;
			if let Some(bytes) = output_bytes {
				let tgon = String::from_utf8(bytes)
					.map_err(|source| tg::error!(!source, "failed to decode the output xattr"))?;
				output.output = Some(
					tgon.parse::<tg::Value>()
						.map_err(|source| tg::error!(!source, "failed to parse the output xattr"))?
						.to_data(),
				);
			}

			let error_bytes = xattr::get(&output_path, "user.tangram.error")
				.map_err(|source| tg::error!(!source, "failed to read the error xattr"))?;
			if let Some(bytes) = error_bytes {
				let error = if let Ok(error) =
					serde_json::from_slice::<tg::Either<tg::error::Data, tg::error::Id>>(&bytes)
				{
					match error {
						tg::Either::Left(data) => tg::Error::try_from(data).map_err(|source| {
							tg::error!(!source, "failed to convert the error data")
						})?,
						tg::Either::Right(id) => tg::Error::with_id(id),
					}
				} else {
					let id = String::from_utf8(bytes).map_err(|source| {
						tg::error!(!source, "failed to decode the error xattr")
					})?;
					let id = id
						.parse()
						.map_err(|source| tg::error!(!source, "failed to parse the error xattr"))?;
					tg::Error::with_id(id)
				};
				output.error = Some(error.to_data_or_id());
			}
		}

		if output.output.is_none() && exists {
			let artifact = tg::checkin::checkin_with_handle(
				&handle,
				tg::checkin::Arg {
					options: tg::checkin::Options {
						destructive: true,
						deterministic: true,
						ignore: false,
						lock: None,
						locked: true,
						root: true,
						..Default::default()
					},
					path: output_path.clone(),
					updates: Vec::new(),
				},
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			output.output = Some(tg::Value::from(artifact).to_data());
		}

		Ok(output)
	}
}

impl tg::Client {
	pub async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>
		+ Send
		+ 'static,
	> {
		let method = http::Method::POST;
		let uri = "/processes/spawn";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let stream = response
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			});
		Ok(stream)
	}
}

async fn checkout_artifacts<H>(
	handle: &H,
	command: &tg::command::Data,
) -> tg::Result<BTreeMap<tg::artifact::Id, PathBuf>>
where
	H: tg::Handle,
{
	let mut objects = BTreeSet::new();
	command.children(&mut objects);
	let artifacts = objects
		.into_iter()
		.filter_map(|object| object.try_into().ok())
		.collect::<BTreeSet<tg::artifact::Id>>();
	let mut output = BTreeMap::new();
	for artifact in artifacts {
		let path = tg::checkout::checkout_with_handle(
			handle,
			tg::checkout::Arg {
				artifact: artifact.clone(),
				dependencies: true,
				extension: None,
				force: false,
				lock: None,
				path: None,
			},
		)
		.await
		.map_err(|source| tg::error!(!source, %artifact, "failed to check out the artifact"))?;
		output.insert(artifact, path);
	}
	Ok(output)
}

fn render_command(
	command: &tg::command::Data,
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
	output_path: &Path,
) -> tg::Result<(PathBuf, Vec<String>)> {
	match command.host.as_str() {
		"builtin" => {
			let mut args = render_args_dash_a(&command.args);
			args.insert(0, "builtin".to_owned());
			args.insert(1, command.executable.to_string());
			Ok(("tangram".into(), args))
		},
		"js" => {
			let mut args = render_args_dash_a(&command.args);
			args.insert(0, "js".to_owned());
			args.insert(1, command.executable.to_string());
			Ok(("tangram".into(), args))
		},
		_ => {
			let executable = render_executable(command, artifacts)?;
			let args = render_args_string(&command.args, artifacts, output_path)?;
			Ok((executable, args))
		},
	}
}

fn render_executable(
	command: &tg::command::Data,
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
) -> tg::Result<PathBuf> {
	match &command.executable {
		tg::command::data::Executable::Artifact(executable) => {
			let mut path = artifacts
				.get(&executable.artifact)
				.cloned()
				.ok_or_else(|| tg::error!("failed to find the executable artifact path"))?;
			if let Some(executable_path) = &executable.path {
				path.push(executable_path);
			}
			Ok(path)
		},
		tg::command::data::Executable::Module(_) => Err(tg::error!("invalid executable")),
		tg::command::data::Executable::Path(executable) => Ok(executable.path.clone()),
	}
}

fn render_args_string(
	args: &[tg::value::Data],
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
	output_path: &Path,
) -> tg::Result<Vec<String>> {
	args.iter()
		.map(|value| render_value_string(value, artifacts, output_path))
		.collect()
}

fn render_args_dash_a(args: &[tg::value::Data]) -> Vec<String> {
	args.iter()
		.flat_map(|value| {
			let value = tg::Value::try_from_data(value.clone()).unwrap().to_string();
			["-A".to_owned(), value]
		})
		.collect()
}

fn render_env(
	env: &tg::value::data::Map,
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
	output_path: &Path,
) -> tg::Result<BTreeMap<String, String>> {
	for key in env.keys() {
		if key.starts_with(tg::process::env::PREFIX) {
			return Err(tg::error!(
				key = %key,
				"env vars prefixed with TANGRAM_ENV_ are reserved"
			));
		}
	}
	let mut resolved = tg::value::data::Map::new();
	for (key, value) in env {
		let mutation = match value {
			tg::value::Data::Mutation(value) => value.clone(),
			value => tg::mutation::Data::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(&mut resolved, key)?;
	}
	let mut output = resolved
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = render_value_string(value, artifacts, output_path)?;
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<BTreeMap<_, _>>>()?;
	for (key, value) in &resolved {
		if matches!(value, tg::value::Data::String(_)) {
			continue;
		}
		let value = tg::Value::try_from_data(value.clone()).unwrap().to_string();
		output.insert(format!("{}{key}", tg::process::env::PREFIX), value);
	}
	output.insert(
		"TANGRAM_OUTPUT".to_owned(),
		output_path.to_string_lossy().into_owned(),
	);
	Ok(output)
}

fn render_value_string(
	value: &tg::value::Data,
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
	output_path: &Path,
) -> tg::Result<String> {
	match value {
		tg::value::Data::String(string) => Ok(string.clone()),
		tg::value::Data::Object(object) if object.is_artifact() => {
			let artifact: tg::artifact::Id = object.clone().try_into()?;
			Ok(artifacts
				.get(&artifact)
				.ok_or_else(|| tg::error!("failed to find the artifact path"))?
				.to_string_lossy()
				.into_owned())
		},
		tg::value::Data::Template(template) => template.try_render(|component| match component {
			tg::template::data::Component::String(string) => Ok(string.clone().into()),
			tg::template::data::Component::Artifact(artifact) => Ok(artifacts
				.get(artifact)
				.ok_or_else(|| tg::error!("failed to find the artifact path"))?
				.to_string_lossy()
				.into_owned()
				.into()),
			tg::template::data::Component::Placeholder(placeholder) => {
				if placeholder.name == "output" {
					Ok(output_path.to_string_lossy().into_owned().into())
				} else {
					Err(tg::error!(name = %placeholder.name, "invalid placeholder"))
				}
			},
		}),
		tg::value::Data::Placeholder(placeholder) => {
			if placeholder.name == "output" {
				Ok(output_path.to_string_lossy().into_owned())
			} else {
				Err(tg::error!(name = %placeholder.name, "invalid placeholder"))
			}
		},
		_ => Ok(tg::Value::try_from_data(value.clone()).unwrap().to_string()),
	}
}

fn convert_stdio(
	stdio: &tg::process::Stdio,
	stream: tg::process::stdio::Stream,
) -> tg::Result<std::process::Stdio> {
	match stdio {
		tg::process::Stdio::Blob(_) => Err(tg::error!(
			stream = %stream,
			"blob stdio is not supported for unsandboxed processes"
		)),
		tg::process::Stdio::Inherit => Ok(std::process::Stdio::inherit()),
		tg::process::Stdio::Log => Err(tg::error!(
			stream = %stream,
			"log stdio is not supported for unsandboxed processes"
		)),
		tg::process::Stdio::Null => Ok(std::process::Stdio::null()),
		tg::process::Stdio::Pipe => Ok(std::process::Stdio::piped()),
		tg::process::Stdio::Tty => Err(tg::error!(
			stream = %stream,
			"tty stdio is not supported for unsandboxed processes"
		)),
	}
}

fn exit_status_to_code(status: std::process::ExitStatus) -> tg::Result<u8> {
	if let Some(code) = status.code() {
		return u8::try_from(code)
			.map_err(|source| tg::error!(!source, "failed to convert the exit code"));
	}
	if let Some(signal) = status.signal() {
		let code = signal
			.checked_add(128)
			.ok_or_else(|| tg::error!("failed to convert the signal"))?;
		return u8::try_from(code)
			.map_err(|source| tg::error!(!source, "failed to convert the signal"));
	}
	Err(tg::error!("failed to determine the exit status"))
}
