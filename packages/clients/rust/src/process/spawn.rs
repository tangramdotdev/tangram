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
		os::unix::process::ExitStatusExt as _,
		path::{Path, PathBuf},
		sync::{Arc, Mutex, RwLock},
		time::Duration,
	},
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{is_default, is_false},
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cached: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache_location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::Checksum>,

	pub command: tg::Referent<tg::command::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub debug: Option<tg::process::Debug>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub public: bool,

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

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lease: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	pub process: tg::Either<u32, tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub wait: Option<tg::process::wait::Output>,
}

pub(super) struct PrepareUnsandboxedCommandOutput {
	pub args: Vec<String>,
	pub cwd: Option<PathBuf>,
	pub env: BTreeMap<String, String>,
	pub executable: PathBuf,
	pub output_path: PathBuf,
	pub temp: tangram_util::fs::Temp,
}

pub async fn spawn(arg: tg::process::Arg) -> tg::Result<tg::Process> {
	let handle = tg::handle()?;
	spawn_with_handle(handle, arg).await
}

pub async fn spawn_with_handle<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<tg::Process>
where
	H: tg::Handle,
{
	tg::Process::<tg::Value>::spawn_with_handle(handle, arg).await
}

pub(crate) async fn spawn_arg_with_handle<H>(
	handle: &H,
	arg: tg::process::Arg,
) -> tg::Result<tg::process::spawn::Arg>
where
	H: tg::Handle,
{
	let sandbox = normalize_sandbox(&arg)?;
	let sandboxed = sandbox.is_some();

	let host = if let Some(host) = arg.host {
		Some(host)
	} else if sandboxed {
		Some(tg::host::current().to_owned())
	} else {
		None
	};
	let mut command_ = None;
	let mut options = tg::referent::Options::default();
	if let Some(command) = arg.command {
		options = command.options;
		command_.replace(command.item);
	}
	if let Some(name) = arg.name {
		options.name.replace(name);
	}
	let executable = if let Some(mut executable) = arg.executable {
		if let tg::command::Executable::Module(executable) = &mut executable {
			let mut module_options = std::mem::take(&mut executable.module.referent.options);
			if options.artifact.is_some() {
				module_options.artifact = options.artifact;
			}
			if options.id.is_some() {
				module_options.id = options.id;
			}
			if options.name.is_some() {
				module_options.name = options.name;
			}
			if options.path.is_some() {
				module_options.path = options.path;
			}
			if options.tag.is_some() {
				module_options.tag = options.tag;
			}
			options = module_options;
		}
		Some(executable)
	} else {
		None
	};
	let checksum = arg.checksum;
	let (process_stdin, command_stdin) = match arg.stdin {
		tg::process::Stdio::Blob(blob) => {
			(tg::process::Stdio::Inherit, Some(tg::Blob::with_id(blob)))
		},
		stdin => (stdin, None),
	};
	let stdout = arg.stdout;
	let stderr = arg.stderr;
	let tty = arg.tty;

	let mut command_has_cwd = false;
	let mut builder = if let Some(command_) = command_ {
		let object = command_.object_with_handle(handle).await?;
		command_has_cwd = object.cwd.is_some();
		tg::command::Builder::with_object(&object)
	} else {
		tg::Command::builder()
	};

	builder = builder.args(arg.args);

	if let Some(cwd) = arg.cwd {
		builder = builder.cwd(cwd);
	} else if !sandboxed && !command_has_cwd {
		let cwd = std::env::current_dir()
			.map_err(|error| tg::error!(!error, "failed to get the current directory"))?;
		builder = builder.cwd(cwd);
	}

	let mut env = tg::value::Map::new();
	if !sandboxed {
		env.extend(tg::process::env()?);
	}
	for (key, value) in arg.env {
		if let Ok(mutation) = value.try_unwrap_mutation_ref() {
			mutation.apply(&mut env, &key)?;
		} else {
			env.insert(key, value);
		}
	}
	builder = builder.env(env);

	if let Some(executable) = executable {
		builder = builder.executable(executable);
	}
	if let Some(host) = host {
		builder = builder.host(host);
	}
	if let Some(user) = arg.user {
		builder = builder.user(user);
	}
	if let Some(command_stdin) = command_stdin {
		builder = builder.stdin(command_stdin);
	}

	let command = builder.finish()?;
	let command_id = command.store_with_handle(handle).await?;
	let command = tg::Referent::new(command_id, options);

	let spawn_arg = tg::process::spawn::Arg {
		cached: arg.cached,
		cache_location: arg.cache_location,
		checksum,
		command,
		debug: normalize_debug(arg.debug),
		location: arg.location,
		parent: None,
		public: arg.public,
		retry: arg.retry,
		sandbox,
		stderr,
		stdin: process_stdin,
		stdout,
		tty,
	};

	Ok(spawn_arg)
}

impl<O: 'static> tg::Process<O> {
	pub async fn spawn(arg: tg::process::Arg) -> tg::Result<tg::Process<O>>
	where
		O: 'static,
	{
		let handle = tg::handle()?;
		Self::spawn_with_handle(handle, arg).await
	}

	pub async fn spawn_with_handle<H>(
		handle: &H,
		arg: tg::process::Arg,
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

	pub async fn spawn_with_progress<F, Fut>(
		arg: tg::process::Arg,
		progress: F,
	) -> tg::Result<tg::Process<O>>
	where
		F: FnOnce(
			BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		) -> Fut,
		Fut: Future<Output = tg::Result<tg::process::spawn::Output>>,
	{
		let handle = tg::handle()?;
		Self::spawn_with_progress_with_handle(handle, arg, progress).await
	}

	pub async fn spawn_with_progress_with_handle<H, F, Fut>(
		handle: &H,
		arg: tg::process::Arg,
		progress: F,
	) -> tg::Result<tg::Process<O>>
	where
		H: tg::Handle,
		F: FnOnce(
			BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		) -> Fut,
		Fut: Future<Output = tg::Result<tg::process::spawn::Output>>,
	{
		let arg = spawn_arg_with_handle(handle, arg).await?;
		Self::spawn_inner_with_handle(handle, arg, progress).await
	}

	async fn spawn_inner_with_handle<H, F, Fut>(
		handle: &H,
		mut arg: tg::process::spawn::Arg,
		progress: F,
	) -> tg::Result<tg::Process<O>>
	where
		H: tg::Handle,
		F: FnOnce(
			BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		) -> Fut,
		Fut: Future<Output = tg::Result<tg::process::spawn::Output>>,
	{
		let handle = handle.clone();
		let sandboxed = arg.sandbox.is_some();
		if !sandboxed {
			let process = Self::spawn_unsandboxed(&handle, arg).await?;
			let output = tg::process::spawn::Output {
				cached: process.cached().unwrap_or(false),
				lease: process.lease().cloned(),
				location: process
					.location()
					.and_then(|location| location.to_location()),
				process: process.id().cloned(),
				wait: None,
			};
			let stream = stream::once(future::ok(tg::progress::Event::Output(output))).boxed();
			progress(stream).await?;
			return Ok(process);
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
		let stdin_is_tty = tangram_util::tty::is_tty(libc::STDIN_FILENO);
		let stdin_is_foreground_controlling_tty =
			tangram_util::tty::is_foreground_controlling_tty(libc::STDIN_FILENO);
		let stdout_is_foreground_controlling_tty =
			tangram_util::tty::is_foreground_controlling_tty(libc::STDOUT_FILENO);
		let stderr_is_foreground_controlling_tty =
			tangram_util::tty::is_foreground_controlling_tty(libc::STDERR_FILENO);
		let resolve_inherited_stdio =
			|stdio: &mut tg::process::Stdio,
			 foreground_tty: bool,
			 background: tg::process::Stdio| {
				if !sandboxed || !stdio.is_inherit() {
					return None;
				}
				let resolved = if !no_tty && foreground_tty {
					tg::process::Stdio::Tty
				} else {
					background
				};
				*stdio = resolved.clone();
				match resolved {
					tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Some(resolved),
					tg::process::Stdio::Null => None,
					_ => unreachable!(),
				}
			};
		let mut tty = match arg.tty.take() {
			Some(tg::Either::Left(true)) => {
				super::stdio::get_tty_size().map(|size| tg::process::Tty { size })
			},
			Some(tg::Either::Right(tty)) => Some(tty),
			_ => None,
		};
		let stdin = resolve_inherited_stdio(
			&mut arg.stdin,
			stdin_is_foreground_controlling_tty,
			if stdin_is_tty {
				tg::process::Stdio::Null
			} else {
				tg::process::Stdio::Pipe
			},
		);
		let stdout = resolve_inherited_stdio(
			&mut arg.stdout,
			stdout_is_foreground_controlling_tty,
			tg::process::Stdio::Pipe,
		);
		let stderr = resolve_inherited_stdio(
			&mut arg.stderr,
			stderr_is_foreground_controlling_tty,
			tg::process::Stdio::Pipe,
		);
		let raw = matches!(stdin, Some(tg::process::Stdio::Tty));
		if tty.is_none()
			&& (stdin.as_ref().is_some_and(tg::process::Stdio::is_tty)
				|| stdout.as_ref().is_some_and(tg::process::Stdio::is_tty)
				|| stderr.as_ref().is_some_and(tg::process::Stdio::is_tty))
		{
			tty = super::stdio::get_tty_size().map(|size| tg::process::Tty { size });
		}
		let local_tty = tty.is_some()
			&& (stdin_is_foreground_controlling_tty
				|| stdout_is_foreground_controlling_tty
				|| stderr_is_foreground_controlling_tty);
		arg.tty = tty.map(tg::Either::Right);
		if arg.stdin.is_tty() || arg.stdout.is_tty() || arg.stderr.is_tty() {
			let mut object = tg::Command::with_id(arg.command.item.clone())
				.object_with_handle(&handle)
				.await
				.map_err(|error| tg::error!(!error, "failed to load the command"))?
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
					.map_err(|error| tg::error!(!error, "failed to store the command"))?;
				arg.command.item = id;
			}
		}
		let stream = handle.spawn_process(arg).await?.boxed();
		let output = progress(stream).await?;
		let wait = output
			.wait
			.map(tg::process::Wait::try_from_data)
			.transpose()?;
		let id = output
			.process
			.as_ref()
			.right()
			.cloned()
			.ok_or_else(|| tg::error!("expected a sandboxed process id"))?;
		let location = output.location.clone();
		let stdio_task = if stdin.is_some() || stdout.is_some() || stderr.is_some() || local_tty {
			let handle = handle.clone();
			let id = id.clone();
			let location = location.clone();
			let stdin = stdin.clone();
			let stdout = stdout.clone();
			let stderr = stderr.clone();
			Some(tangram_futures::task::Shared::spawn(move |_| async move {
				let arg = super::stdio::StdioTaskArg {
					handle,
					id,
					location,
					stdin,
					stdout,
					stderr,
					tty: local_tty,
					raw,
				};
				super::stdio::stdio_task(arg).await
			}))
		} else {
			None
		};
		let stderr = if provide_stderr {
			super::stdio::Reader::from_process(tg::process::stdio::Stream::Stderr)
		} else {
			super::stdio::Reader::unavailable(tg::process::stdio::Stream::Stderr)
		};
		let stdin = if provide_stdin {
			super::stdio::Writer::from_process(tg::process::stdio::Stream::Stdin)
		} else {
			super::stdio::Writer::unavailable(tg::process::stdio::Stream::Stdin)
		};
		let stdout = if provide_stdout {
			super::stdio::Reader::from_process(tg::process::stdio::Stream::Stdout)
		} else {
			super::stdio::Reader::unavailable(tg::process::stdio::Stream::Stdout)
		};
		let inner = Arc::new(super::Inner {
			cached: Some(output.cached),
			id: tg::Either::Right(id),
			lease: output.lease,
			location: Arc::new(RwLock::new(location.map(Into::into))),
			metadata: RwLock::new(None),
			state: RwLock::new(None),
			stderr,
			stdin,
			stdio_task,
			stdout,
			task: None,
			token: RwLock::new(None),
			wait: Mutex::new(wait),
		});
		let process = Self(inner, std::marker::PhantomData);
		process.stdin().set_process(Arc::downgrade(&process.0));
		process.stdout().set_process(Arc::downgrade(&process.0));
		process.stderr().set_process(Arc::downgrade(&process.0));
		Ok(process)
	}

	pub(super) async fn prepare_unsandboxed_command<H>(
		handle: &H,
		arg: &tg::process::spawn::Arg,
		output_path: Option<PathBuf>,
	) -> tg::Result<PrepareUnsandboxedCommandOutput>
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
			.map_err(|error| tg::error!(!error, "failed to load the command"))?;
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

		let temp = tangram_util::fs::Temp::new()
			.map_err(|error| tg::error!(!error, "failed to create a temp directory"))?;
		tokio::fs::create_dir(temp.path())
			.await
			.map_err(|error| tg::error!(!error, "failed to create a temp directory"))?;
		let output_path = output_path.unwrap_or_else(|| temp.path().join("output"));
		let artifacts = checkout_artifacts(handle, &command).await?;
		let env = render_env(handle, &command.env, &artifacts, &output_path)?;
		let (executable, args) =
			render_command(&command, &artifacts, &output_path, arg.debug.as_ref())?;
		let cwd = command.cwd.clone();

		let output = PrepareUnsandboxedCommandOutput {
			args,
			cwd,
			env,
			executable,
			output_path,
			temp,
		};

		Ok(output)
	}

	async fn spawn_unsandboxed<H>(
		handle: &H,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<tg::Process<O>>
	where
		H: tg::Handle,
	{
		let prepared = Self::prepare_unsandboxed_command(handle, &arg, None).await?;

		let executable = resolve_executable(&prepared.executable, &prepared.env)?;
		let mut command_ = tokio::process::Command::new(&executable);
		command_.args(&prepared.args);
		command_.env_clear();
		command_.envs(&prepared.env);
		if let Some(cwd) = &prepared.cwd {
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

		let mut child = command_.spawn().map_err(|error| {
			tg::error!(
				!error,
				executable = %prepared.executable.display(),
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
			let output_path = prepared.output_path;
			let temp = prepared.temp;
			move |_| async move { Self::wait_unsandboxed(handle, child, output_path, temp).await }
		});
		task.detach();

		let inner = Arc::new(super::Inner {
			cached: Some(false),
			id: tg::Either::Left(pid),
			lease: None,
			location: Arc::new(RwLock::new(None)),
			metadata: RwLock::new(None),
			state: RwLock::new(None),
			stderr,
			stdin,
			stdio_task: None,
			stdout,
			task: Some(task),
			token: RwLock::new(None),
			wait: Mutex::new(None),
		});
		let process = Self(inner, std::marker::PhantomData);
		process.stdin().set_process(Arc::downgrade(&process.0));
		process.stdout().set_process(Arc::downgrade(&process.0));
		process.stderr().set_process(Arc::downgrade(&process.0));
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
			.map_err(|error| tg::error!(!error, "failed to wait for the process"))?;
		let exit = exit_status_to_code(status)?;
		let mut output = tg::process::wait::Output {
			error: None,
			exit,
			output: None,
		};

		let exists = tokio::fs::try_exists(&output_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to determine if the output path exists"))?;

		if exists {
			let output_bytes = xattr::get(&output_path, "user.tangram.output")
				.map_err(|error| tg::error!(!error, "failed to read the output xattr"))?;
			if let Some(bytes) = output_bytes {
				let tgon = String::from_utf8(bytes)
					.map_err(|error| tg::error!(!error, "failed to decode the output xattr"))?;
				output.output = Some(
					tgon.parse::<tg::Value>()
						.map_err(|error| tg::error!(!error, "failed to parse the output xattr"))?
						.to_data(),
				);
			}

			let error_bytes = xattr::get(&output_path, "user.tangram.error")
				.map_err(|error| tg::error!(!error, "failed to read the error xattr"))?;
			if let Some(bytes) = error_bytes {
				let error = if let Ok(error) =
					serde_json::from_slice::<tg::Either<tg::error::Data, tg::error::Id>>(&bytes)
				{
					match error {
						tg::Either::Left(data) => tg::Error::try_from(data).map_err(|error| {
							tg::error!(!error, "failed to convert the error data")
						})?,
						tg::Either::Right(id) => tg::Error::with_id(id),
					}
				} else {
					let id = String::from_utf8(bytes)
						.map_err(|error| tg::error!(!error, "failed to decode the error xattr"))?;
					let id = id
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse the error xattr"))?;
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
			.map_err(|error| tg::error!(!error, "failed to check in the output"))?;
			output.output = Some(tg::Value::from(artifact).to_data());
		}

		Ok(output)
	}
}

impl tg::Session {
	pub async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>
		+ Send
		+ 'static
		+ use<>,
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
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let stream = response
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read an event"))
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
		.map_err(|error| tg::error!(!error, %artifact, "failed to check out the artifact"))?;
		output.insert(artifact, path);
	}
	Ok(output)
}

fn render_command(
	command: &tg::command::Data,
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
	output_path: &Path,
	debug: Option<&tg::process::Debug>,
) -> tg::Result<(PathBuf, Vec<String>)> {
	match (&command.executable, command.host.as_str()) {
		(_, "builtin") => {
			let mut args = render_args_dash_a(&command.args);
			args.insert(0, "builtin".to_owned());
			args.insert(1, command.executable.to_string());
			Ok(("tangram".into(), args))
		},
		(tg::command::data::Executable::Module(_), _) | (_, "js") => {
			let mut args = Vec::new();
			args.push("js".to_owned());
			args.push("--host".to_owned());
			args.push(command.host.clone());
			args.extend(render_js_debug_args(debug));
			args.push(command.executable.to_string());
			args.extend(render_args_dash_a(&command.args));
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

pub(super) fn resolve_executable(
	executable: &Path,
	env: &BTreeMap<String, String>,
) -> tg::Result<PathBuf> {
	if executable.is_absolute() || executable.components().count() > 1 {
		return Ok(executable.to_owned());
	}
	let path = env.get("PATH").ok_or_else(|| {
		tg::error!(
			executable = %executable.display(),
			"failed to find the executable in PATH"
		)
	})?;
	which(Path::new(path), executable).ok_or_else(|| {
		tg::error!(
			executable = %executable.display(),
			"failed to find the executable in PATH"
		)
	})
}

fn which(path: &Path, executable: &Path) -> Option<PathBuf> {
	if executable.is_absolute() {
		return Some(executable.to_owned());
	}
	for directory in std::env::split_paths(path) {
		let candidate = directory.join(executable);
		if candidate.is_file() {
			return Some(candidate);
		}
	}
	None
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

fn render_js_debug_args(debug: Option<&tg::process::Debug>) -> Vec<String> {
	let mut args = Vec::new();
	let Some(debug) = debug else {
		return args;
	};
	args.push("--debug".to_owned());
	if let Some(addr) = debug.addr {
		args.push("--debug-addr".to_owned());
		args.push(addr.to_string());
	}
	if debug.mode != tg::process::debug::Mode::Normal {
		args.push("--debug-mode".to_owned());
		args.push(debug.mode.to_string());
	}
	args
}

fn render_env<H>(
	handle: &H,
	env: &tg::value::data::Map,
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
	output_path: &Path,
) -> tg::Result<BTreeMap<String, String>>
where
	H: tg::Handle,
{
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
	for key in [
		"TANGRAM_CONFIG",
		"TANGRAM_DIRECTORY",
		"TANGRAM_MODE",
		"TANGRAM_OUTPUT",
		"TANGRAM_TOKEN",
		"TANGRAM_TRACING",
		"TANGRAM_URL",
	] {
		output.remove(key);
	}
	output.insert(
		"TANGRAM_OUTPUT".to_owned(),
		output_path.to_string_lossy().into_owned(),
	);
	let arg = handle.arg();
	if let Some(token) = arg.token {
		output.insert("TANGRAM_TOKEN".to_owned(), token);
	}
	if let Some(url) = arg.url {
		output.insert("TANGRAM_URL".to_owned(), url.to_string());
	}
	Ok(output)
}

fn render_value_string(
	value: &tg::value::Data,
	artifacts: &BTreeMap<tg::artifact::Id, PathBuf>,
	output_path: &Path,
) -> tg::Result<String> {
	match value {
		tg::value::Data::String(string) => Ok(string.clone()),
		tg::value::Data::Object(object)
			if object
				.clone()
				.map_right(|object| object.id)
				.into_inner()
				.is_artifact() =>
		{
			let artifact: tg::artifact::Id = object
				.clone()
				.map_right(|object| object.id)
				.into_inner()
				.try_into()
				.unwrap();
			Ok(artifacts
				.get(&artifact)
				.ok_or_else(|| tg::error!("failed to find the artifact path"))?
				.to_string_lossy()
				.into_owned())
		},
		tg::value::Data::Template(template) => template.try_render(|component| match component {
			tg::template::data::Component::String(string) => Ok(string.clone().into()),
			tg::template::data::Component::Artifact(artifact) => Ok(artifacts
				.get(
					&artifact
						.clone()
						.map_right(|artifact| artifact.id)
						.into_inner(),
				)
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
			.map_err(|error| tg::error!(!error, "failed to convert the exit code"));
	}
	if let Some(signal) = status.signal() {
		let code = signal
			.checked_add(128)
			.ok_or_else(|| tg::error!("failed to convert the signal"))?;
		return u8::try_from(code)
			.map_err(|error| tg::error!(!error, "failed to convert the signal"));
	}
	Err(tg::error!("failed to determine the exit status"))
}

fn normalize_sandbox(
	arg: &tg::process::Arg,
) -> tg::Result<Option<tg::Either<tg::sandbox::create::Arg, tg::sandbox::Id>>> {
	let has_cpu = arg.cpu.is_some();
	let cpu = arg.cpu;
	let has_memory = arg.memory.is_some();
	let memory = arg.memory;
	let mounts = arg.mounts.clone();
	let ports = arg.ports.clone();
	let has_network = arg.network.is_some();
	let has_owner = arg.owner.is_some();
	let has_ports = !ports.is_empty();
	let network = normalize_network(arg.network.clone(), ports.clone())?;
	let has_sandbox_fields =
		has_cpu || has_memory || !mounts.is_empty() || has_network || has_owner || has_ports;
	match arg.sandbox.clone() {
		Some(tg::process::SandboxArg::Bool(true)) => {
			let mut sandbox = tg::process::SandboxCreateArg::default();
			if let Some(cpu) = cpu {
				sandbox.cpu = Some(cpu);
			}
			if let Some(memory) = memory {
				sandbox.memory = Some(memory);
			}
			if !mounts.is_empty() {
				sandbox.mounts.extend(mounts);
			}
			if has_network || has_ports {
				sandbox.network = network.clone();
			}
			if let Some(owner) = arg.owner.clone() {
				sandbox.owner = Some(owner);
			}
			let sandbox = normalize_sandbox_create_arg(sandbox);
			Ok(Some(tg::Either::Left(sandbox)))
		},
		Some(tg::process::SandboxArg::Arg(mut sandbox)) => {
			if let Some(cpu) = cpu {
				sandbox.cpu = Some(cpu);
			}
			if let Some(memory) = memory {
				sandbox.memory = Some(memory);
			}
			if !mounts.is_empty() {
				sandbox.mounts.extend(mounts);
			}
			if has_network {
				sandbox.network = network.clone();
			} else if has_ports {
				sandbox.network = normalize_network(sandbox.network, ports)?;
			}
			if let Some(owner) = arg.owner.clone() {
				sandbox.owner = Some(owner);
			}
			let sandbox = normalize_sandbox_create_arg(sandbox);
			Ok(Some(tg::Either::Left(sandbox)))
		},
		Some(tg::process::SandboxArg::Id(sandbox)) => {
			if has_sandbox_fields {
				return Err(tg::error!(
					"cpu, memory, mounts, network, owner, and ports are not supported for existing sandboxes"
				));
			}
			Ok(Some(tg::Either::Right(sandbox)))
		},
		None | Some(tg::process::SandboxArg::Bool(false)) => {
			if !has_sandbox_fields {
				return Ok(None);
			}
			let sandbox = tg::sandbox::create::Arg {
				cpu,
				hostname: None,
				isolation: None,
				location: None,
				memory,
				mounts,
				network,
				owner: arg.owner.clone(),
				ttl: Some(Duration::ZERO),
			};
			Ok(Some(tg::Either::Left(sandbox)))
		},
	}
}

fn normalize_sandbox_create_arg(
	sandbox: tg::process::SandboxCreateArg,
) -> tg::sandbox::create::Arg {
	tg::sandbox::create::Arg {
		cpu: sandbox.cpu,
		hostname: sandbox.hostname,
		isolation: sandbox.isolation,
		location: sandbox.location,
		memory: sandbox.memory,
		mounts: sandbox.mounts,
		network: sandbox.network,
		owner: sandbox.owner,
		ttl: sandbox.ttl.unwrap_or(Some(Duration::ZERO)),
	}
}

fn normalize_network(
	network: Option<tg::sandbox::Network>,
	ports: Vec<tg::sandbox::Port>,
) -> tg::Result<Option<tg::sandbox::Network>> {
	if ports.is_empty() {
		return Ok(network);
	}
	match network {
		Some(tg::sandbox::Network::Host) => {
			Err(tg::error!("ports are not supported with host networking"))
		},
		Some(tg::sandbox::Network::Bridge(mut bridge)) => {
			bridge.ports.extend(ports);
			Ok(Some(tg::sandbox::Network::Bridge(bridge)))
		},
		Some(tg::sandbox::Network::Default) | None => {
			Ok(Some(tg::sandbox::Network::Bridge(tg::sandbox::Bridge {
				ports,
			})))
		},
	}
}

fn normalize_debug(
	debug: Option<tg::Either<bool, tg::process::Debug>>,
) -> Option<tg::process::Debug> {
	match debug {
		None | Some(tg::Either::Left(false)) => None,
		Some(tg::Either::Left(true)) => Some(tg::process::Debug::default()),
		Some(tg::Either::Right(debug)) => Some(debug),
	}
}
