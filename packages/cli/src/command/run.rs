use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{future, stream, AsyncRead, Stream, StreamExt, TryFutureExt as _, TryStreamExt as _};
use itertools::Itertools as _;
use std::{
	io::{IsTerminal as _, Read as _, StderrLock, Write},
	path::{Path, PathBuf},
	pin::pin,
};
use tangram_client::{self as tg, handle::Ext as _, Handle as _};
use tangram_either::Either;
use tangram_futures::task::{Stop, Task};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_stream::wrappers::ReceiverStream;

/// Run a command.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub inner: crate::command::run::InnerArgs,

	/// The reference to the command to run.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Arguments to pass to the executable.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct InnerArgs {
	/// Set the arguments.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<String>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	/// If false, don't create a new process.
	#[arg(default_value = "true", long, action = clap::ArgAction::Set)]
	pub create: bool,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long, conflicts_with = "checkout")]
	pub detach: bool,

	/// Set the environment variables.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub env: Vec<Vec<String>>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The remote to use.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// Whether to retry failed processes.
	#[arg(long)]
	pub retry: bool,

	/// Tag the process.
	#[arg(long)]
	pub tag: Option<tg::Tag>,

	/// Choose the view.
	#[arg(long, default_value = "inline")]
	pub view: View,
}

pub enum InnerKind {
	Build,
	Run,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum View {
	None,
	#[default]
	Inline,
	Fullscreen,
}

#[derive(Clone, Debug, derive_more::Unwrap)]
pub enum InnerOutput {
	Detached(tg::process::Id),
	Path(PathBuf),
	Value(tg::Value),
}

impl Cli {
	pub async fn command_command_run(&self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let reference = args
			.reference
			.clone()
			.unwrap_or_else(|| ".".parse().unwrap());

		// Run the command.
		let kind = InnerKind::Run;
		let output = self.command_run_inner(reference, kind, args.inner).await?;

		// Print the output.
		match output {
			crate::command::run::InnerOutput::Detached(process) => {
				println!("{process}");
			},
			crate::command::run::InnerOutput::Path(path) => {
				println!("{}", path.display());
			},
			crate::command::run::InnerOutput::Value(value) => {
				if !value.is_null() {
					let stdout = std::io::stdout();
					let value = if stdout.is_terminal() {
						let options = tg::value::print::Options {
							recursive: false,
							style: tg::value::print::Style::Pretty { indentation: "  " },
						};
						value.print(options)
					} else {
						value.to_string()
					};
					println!("{value}");
				}
			},
		}

		Ok(())
	}

	pub(crate) async fn command_run_inner(
		&self,
		reference: tg::Reference,
		kind: InnerKind,
		args: InnerArgs,
	) -> tg::Result<InnerOutput> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// If the reference is a path to a directory and the path does not contain a root module, then init.
		if let Ok(path) = reference.item().try_unwrap_path_ref() {
			let path = if let Some(subpath) = reference
				.options()
				.and_then(|options| options.subpath.as_ref())
			{
				path.join(subpath)
			} else {
				path.clone()
			};
			let metadata = tokio::fs::metadata(&path).await.map_err(
				|source| tg::error!(!source, ?path = path.display(), "failed to get the metadata"),
			)?;
			if metadata.is_dir() {
				let mut exists = false;
				for name in tg::package::ROOT_MODULE_FILE_NAMES {
					let module_path = path.join(name);
					exists = tokio::fs::try_exists(&module_path)
						.await
						.map_err(|source| {
							tg::error!(!source, ?path, "failed to check if the path exists")
						})?;
					if exists {
						break;
					}
				}
				if !exists {
					self.command_package_init(crate::package::init::Args {
						path: Some(path.clone()),
					})
					.await?;
				}
			}
		}

		// Get the reference.
		let referent = self.get_reference(&reference).await?;
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let object = if let Some(subpath) = &referent.subpath {
			let directory = object
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			directory.get(&handle, subpath).await?.into()
		} else {
			object
		};

		// Create the command.
		let command = if let tg::Object::Command(command) = object {
			// If the object is a command, then use it.
			command
		} else {
			// Otherwise, the object must be a directory containing a root module, or a file.
			let executable = match object {
				tg::Object::Directory(directory) => {
					let mut name = None;
					for name_ in tg::package::ROOT_MODULE_FILE_NAMES {
						if directory.try_get_entry(&handle, name_).await?.is_some() {
							name = Some(name_);
							break;
						}
					}
					let name = name.ok_or_else(|| tg::error!("no root module found"))?;
					let kind = if Path::new(name)
						.extension()
						.is_some_and(|extension| extension == "js")
					{
						tg::module::Kind::Js
					} else if Path::new(name)
						.extension()
						.is_some_and(|extension| extension == "ts")
					{
						tg::module::Kind::Ts
					} else {
						unreachable!();
					};
					let item = directory.clone().into();
					let subpath = Some(name.parse().unwrap());
					let referent = tg::Referent {
						item,
						path: referent.path,
						subpath,
						tag: referent.tag,
					};
					let module = tg::command::Module { kind, referent };
					tg::command::Executable::Module(module)
				},

				tg::Object::File(file) => {
					let kind = if let Ok(path) = reference.item().try_unwrap_path_ref() {
						let path = if let Some(subpath) = reference
							.options()
							.and_then(|options| options.subpath.as_ref())
						{
							path.join(subpath)
						} else {
							path.clone()
						};
						if path.extension().is_some_and(|extension| extension == "js") {
							tg::module::Kind::Js
						} else if path.extension().is_some_and(|extension| extension == "ts") {
							tg::module::Kind::Ts
						} else {
							return Err(tg::error!("invalid file extension"));
						}
					} else {
						return Err(tg::error!("cannot determine the file's kind"));
					};
					let referent = tg::Referent::with_item(file.into());
					tg::command::Executable::Module(tg::command::Module { kind, referent })
				},

				_ => {
					return Err(tg::error!("expected a directory or a file"));
				},
			};

			// Get the command.
			let command = reference
				.uri()
				.fragment()
				.map_or("default", |fragment| fragment);

			// Get the args.
			let mut args_: Vec<tg::Value> = args
				.arg
				.into_iter()
				.map(|arg| arg.parse())
				.try_collect::<tg::Value, _, _>()?;
			args_.insert(0, command.into());

			// Get the env.
			let mut env: tg::value::Map = args
				.env
				.into_iter()
				.flatten()
				.map(|env| {
					let map = env
						.parse::<tg::Value>()?
						.try_unwrap_map()
						.map_err(|_| tg::error!("expected a map"))?
						.into_iter();
					Ok::<_, tg::Error>(map)
				})
				.try_fold(tg::value::Map::new(), |mut map, item| {
					map.extend(item?);
					Ok::<_, tg::Error>(map)
				})?;

			// Set the TANGRAM_HOST environment variable if it is not set.
			if !env.contains_key("TANGRAM_HOST") {
				let host = if let Some(host) = args.host {
					host
				} else {
					tg::host().to_owned()
				};
				env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
			}

			// Choose the host.
			let host = "js";

			// Create the command.
			tg::command::Builder::new(host)
				.executable(Some(executable))
				.args(args_)
				.env(env)
				.build()
		};

		// Determine the retry.
		let retry = args.retry;

		// Print the command.
		eprintln!(
			"{} command {}",
			"info".blue().bold(),
			command.id(&handle).await?
		);

		// If the remote is set, then push the commnad.
		if let Some(remote) = remote.clone() {
			let id = command.id(&handle).await?;
			let arg = tg::object::push::Arg { remote };
			let stream = handle.push_object(&id.into(), arg).await?;
			self.render_progress_stream(stream).await?;
		}

		// Handle build vs run.
		let (cwd, sandbox, stdin, stdout, stderr, stdio_task) = match kind {
			InnerKind::Build => {
				let cwd = None;
				let sandbox = Some(tg::process::Sandbox::default());
				let stdin = None;
				let stdout = None;
				let stderr = None;
				let stdio_task = None;
				(cwd, sandbox, stdin, stdout, stderr, stdio_task)
			},
			InnerKind::Run => {
				let cwd = Some(std::env::current_dir().map_err(|source| {
					tg::error!(!source, "failed to get the working directory")
				})?);
				let sandbox = None;
				let (stdin, stdout, stderr) = futures::try_join!(
					handle.open_pipe().map_ok(|output| Some(output.id)),
					handle.open_pipe().map_ok(|output| Some(output.id)),
					handle.open_pipe().map_ok(|output| Some(output.id)),
				)?;
				let stdio_task = Some(spawn_stdio_task(
					&handle,
					stdin.clone(),
					stdout.clone(),
					stderr.clone(),
				));
				(cwd, sandbox, stdin, stdout, stderr, stdio_task)
			},
		};

		// Spawn the process.
		let id = command.id(&handle).await?;
		let arg = tg::command::spawn::Arg {
			checksum: args.checksum,
			create: args.create,
			cwd,
			parent: None,
			remote: remote.clone(),
			retry,
			sandbox,
			stderr: stderr.clone(),
			stdin: stdin.clone(),
			stdout: stdout.clone(),
		};
		let output = handle.spawn_command(&id, arg).await?;
		let process = tg::Process::with_id(output.process);

		// Tag the process if requested.
		if let Some(tag) = args.tag {
			let item = Either::Left(process.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				remote: remote.clone(),
			};
			handle.put_tag(&tag, arg).await?;
		}

		// If the detach flag is set, then return the process.
		if args.detach {
			return Ok(InnerOutput::Detached(process.id().clone()));
		}

		// Print the process.
		eprintln!("{} process {}", "info".blue().bold(), process.id());

		// Get the process's status.
		let status = process
			.status(&handle)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the process is finished, then get the process's output.
		let output = if status.is_finished() {
			let output = process
				.wait(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the output"))?;
			Some(output)
		} else {
			None
		};

		// If the process is not finished, then wait for it to finish while showing the viewer if enabled.
		let result = if let Some(output) = output {
			Ok(output)
		} else {
			// Spawn the view task.
			let view_task = {
				let handle = handle.clone();
				let process = process.clone();
				let task = Task::spawn_blocking(move |stop| {
					let local_set = tokio::task::LocalSet::new();
					let runtime = tokio::runtime::Builder::new_current_thread()
						.worker_threads(1)
						.enable_all()
						.build()
						.unwrap();
					local_set
						.block_on(&runtime, async move {
							let options = crate::viewer::Options {
								condensed_processes: true,
								expand_on_create: true,
							};
							let item = crate::viewer::Item::Process(process);
							let mut viewer = crate::viewer::Viewer::new(&handle, item, options);
							match args.view {
								View::None => (),
								View::Inline => {
									viewer.run_inline(stop).await?;
								},
								View::Fullscreen => {
									viewer.run_fullscreen(stop).await?;
								},
							}
							Ok::<_, tg::Error>(())
						})
						.unwrap();
				});
				Some(task)
			};

			// Spawn a task to attempt to cancel the process on the first interrupt signal and exit the process on the second.
			let cancel_task = tokio::spawn({
				let handle = handle.clone();
				let process = process.clone();
				async move {
					tokio::signal::ctrl_c().await.unwrap();
					tokio::spawn(async move {
						let arg = tg::process::finish::Arg {
							error: Some(tg::error!("the process was explicitly canceled")),
							exit: None,
							output: None,
							remote,
							status: tg::process::Status::Canceled,
						};
						process
							.finish(&handle, arg)
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to cancel the process");
							})
							.ok();
					});
					tokio::signal::ctrl_c().await.unwrap();
					std::process::exit(130);
				}
			});

			// Wait for the process's output.
			let result = process.wait(&handle).await;

			// Abort the cancel task.
			cancel_task.abort();

			// Stop and await the view task.
			if let Some(view_task) = view_task {
				view_task.stop();
				view_task.wait().await.unwrap();
			}

			result
		};

		// Close the pipes.
		for pipe in [stderr, stdin, stdout].into_iter().flatten() {
			handle.close_pipe(&pipe).await.ok();
		}

		// Get the output or return an error if we failed to wait for the process.
		let output =
			result.map_err(|source| tg::error!(!source, "failed to wait for the process"))?;

		// Wait for the stdio task.
		if let Some(stdio_task) = stdio_task {
			stdio_task.stop();
			stdio_task.wait().await.unwrap();
		}

		// Return an error if appropriate.
		if let Some(source) = output.error {
			return Err(tg::error!(!source, "the process failed"));
		}
		if matches!(output.status, tg::process::Status::Canceled) {
			return Err(tg::error!("the process was canceled"));
		}
		match &output.exit {
			Some(tg::process::Exit::Code { code }) if *code != 0 => {
				return Err(tg::error!("the process exited with code {code}"));
			},
			Some(tg::process::Exit::Signal { signal }) => {
				return Err(tg::error!("the process exited with signal {signal}"));
			},
			_ => (),
		}

		// Check out the output if requested.
		if let Some(path) = args.checkout {
			// Get the artifact.
			let artifact: tg::Artifact = output
				.output
				.ok_or_else(|| tg::error!("expected an output value"))?
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;

			// Get the path.
			let path = if let Some(path) = path {
				let path = std::path::absolute(path)
					.map_err(|source| tg::error!(!source, "failed to get the path"))?;
				Some(path)
			} else {
				None
			};

			// Check out the artifact.
			let arg = tg::artifact::checkout::Arg {
				dependencies: path.is_some(),
				force: false,
				lockfile: false,
				path,
			};
			let stream = handle
				.check_out_artifact(&artifact.id(&handle).await?, arg)
				.await?;
			let output = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(InnerOutput::Path(output.path));
		}

		Ok(InnerOutput::Value(output.output.unwrap_or(tg::Value::Null)))
	}
}

impl Default for InnerArgs {
	fn default() -> Self {
		Self {
			arg: vec![],
			checkout: None,
			checksum: None,
			create: true,
			detach: false,
			env: vec![],
			host: None,
			locked: false,
			remote: None,
			retry: false,
			tag: None,
			view: View::default(),
		}
	}
}

fn spawn_stdio_task<H>(
	handle: &H,
	stdin: Option<tg::pipe::Id>,
	stdout: Option<tg::pipe::Id>,
	stderr: Option<tg::pipe::Id>,
) -> Task<()>
where
	H: tg::Handle,
{
	let handle = handle.clone();
	Task::spawn(|stop| async move {
		let stdin = tokio::spawn({
			let handle = handle.clone();
			async move {
				let Some(pipe) = stdin else {
					return;
				};
				let fut = stdin_task(&handle, pipe);
				let stop = stop.wait();
				match future::select(pin!(fut), pin!(stop)).await {
					future::Either::Left((Err(error), ..)) => {
						eprintln!("stdin error: {error}");
					},
					_ => (),
				}
			}
		});

		let stdout = tokio::spawn({
			let handle = handle.clone();
			async move {
				let Some(pipe) = stdout else {
					return;
				};
				if let Err(error) = output_task(&handle, &pipe, tokio::io::stdout()).await {
					eprintln!("stdout error: {error}");
				}
			}
		});

		let stderr = tokio::spawn({
			let handle = handle.clone();
			async move {
				let Some(pipe) = stderr else {
					return;
				};
				if let Err(error) = output_task(&handle, &pipe, tokio::io::stderr()).await {
					eprintln!("stdout error: {error}");
				}
			}
		});

		let (stdin, stdout, stderr) = future::join3(stdin, stdout, stderr).await;
		stdin.unwrap();
		stdout.unwrap();
		stderr.unwrap();
	})
}

async fn stdin_task<H>(handle: &H, pipe: tg::pipe::Id) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Create a send/receive pair for sending stdin chunks. The channel is bounded to 1 to avoid buffering stdin messages.
	let (send, recv) = tokio::sync::mpsc::channel(1);

	// Spawn the stdin thread and detach it. This is necessary because the read from stdin cannot be interrupted or canceled.
	std::thread::spawn(move || {
		let mut stdin = std::io::stdin();
		let mut buf = vec![0u8; 4096];
		loop {
			let result = match stdin.read(&mut buf) {
				Ok(0) => return,
				Ok(n) => Ok(tg::pipe::Event::Chunk(Bytes::copy_from_slice(&buf[0..n]))),
				Err(source) => Err(tg::error!(!source, "failed to read from stdin")),
			};
			if let Err(_error) = send.blocking_send(result) {
				break;
			}
		}
	});

	// Create a stream.
	let stream =
		ReceiverStream::new(recv).chain(stream::once(future::ready(Ok(tg::pipe::Event::End))));

	// Write.
	handle
		.write_pipe(&pipe, stream)
		.await
		.map_err(|source| tg::error!(!source, %pipe, "failed to write pipe"))?;

	Ok(())
}

async fn output_task<H, W>(handle: &H, pipe: &tg::pipe::Id, writer: W) -> tg::Result<()>
where
	H: tg::Handle,
	W: AsyncWrite + Send + 'static,
{
	let mut writer = pin!(writer);

	// Create the stream.
	let stream = handle
		.read_pipe(pipe)
		.await
		.map_err(|source| tg::error!(!source, %pipe, "failed to get pipe read stream"))?;
	let mut stream = pin!(stream);

	// Drain the pipe.
	while let Some(event) = stream
		.try_next()
		.await
		.map_err(|source| tg::error!(%source, "failed to read pipe"))?
	{
		match event {
			tg::pipe::Event::Chunk(chunk) => {
				writer
					.write_all(&chunk)
					.await
					.map_err(|source| tg::error!(!source, "failed to write from pipe"))?;
				writer
					.flush()
					.await
					.map_err(|source| tg::error!(!source, "failed to flush"))?;
				eprintln!("wrote to {pipe}: {chunk:?}");
			},
			tg::pipe::Event::End => {
				flog(format!("end"));
				break;
			}
		}
	}
	eprintln!("done");
	Ok(())
}

fn flog(msg: String) {
	let mut log = std::fs::OpenOptions::new()
		.write(true)
		.append(true)
		.create(true)
		.open("log.txt")
		.unwrap();
	log.write_all(msg.as_bytes()).unwrap();
	log.write_all(b"\n").unwrap();
	log.flush().unwrap();
}