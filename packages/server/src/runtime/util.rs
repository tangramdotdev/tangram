use super::Runtime;
use crate::Server;
use bytes::Bytes;
use futures::{Stream, TryStreamExt as _, future, stream};
use std::{collections::BTreeMap, path::Path, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Stop;
use tangram_sandbox as sandbox;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};

/// Render a value.
pub fn render_value(artifacts_path: &Path, value: &tg::value::Data) -> String {
	if let Ok(string) = value.try_unwrap_string_ref() {
		return string.clone();
	}
	if let Ok(object) = value.try_unwrap_object_ref() {
		if let Ok(artifact) = tg::artifact::Id::try_from(object.clone()) {
			let string = artifacts_path
				.join(artifact.to_string())
				.to_str()
				.unwrap()
				.to_owned();
			return string;
		}
	}
	if let Ok(template) = value.try_unwrap_template_ref() {
		let string = template.render(|component| match component {
			tg::template::component::Data::String(string) => string.clone().into(),
			tg::template::component::Data::Artifact(artifact) => artifacts_path
				.join(artifact.to_string())
				.to_str()
				.unwrap()
				.to_owned()
				.into(),
		});
		return string;
	}
	"<tangram value>".to_owned()
}

pub fn render_env(
	artifacts_path: &Path,
	env: &tg::value::data::Map,
) -> tg::Result<BTreeMap<String, String>> {
	let mut output = BTreeMap::new();
	for (key, value) in env {
		let mutation = match value {
			tg::value::Data::Mutation(value) => value.clone(),
			value => tg::mutation::Data::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(&mut output, key)?;
	}
	let output = output
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = render_value(artifacts_path, value);
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<_>>()?;
	Ok(output)
}

pub async fn compute_checksum(
	runtime: &Runtime,
	process: &tg::Process,
	value: &tg::Value,
	checksum: &tg::Checksum,
) -> tg::Result<tg::Checksum> {
	let algorithm = checksum.algorithm();
	let algorithm = if algorithm == tg::checksum::Algorithm::None {
		tg::checksum::Algorithm::Sha256
	} else {
		algorithm
	};

	if algorithm == tg::checksum::Algorithm::Any {
		return Ok(checksum.clone());
	}

	let host = "builtin";
	let executable = tg::command::Executable::Path("checksum".into());
	let args = vec![value.clone(), algorithm.to_string().into()];
	let command = tg::Command::builder(host, executable).args(args).build();

	// If the process is remote, push the command.
	let remote = process.remote();
	if let Some(remote) = remote {
		let arg = tg::push::Arg {
			items: vec![Either::Right(
				command
					.id(runtime.server())
					.await
					.map_err(|source| tg::error!(!source, "could not find command id"))?
					.into(),
			)],

			remote: remote.to_owned(),
			..Default::default()
		};
		let stream = runtime.server().push(arg).await?;

		// Consume the stream and log progress.
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::progress::Event::Start(indicator) | tg::progress::Event::Update(indicator) => {
					if indicator.name == "bytes" {
						let message = format!("{indicator}\n");
						log(
							runtime.server(),
							process,
							tg::process::log::Stream::Stderr,
							message,
						)
						.await;
					}
				},
				tg::progress::Event::Output(()) => {
					break;
				},
				_ => {},
			}
		}
	}

	let arg = tg::process::spawn::Arg {
		command: Some(command.id(runtime.server()).await?),
		parent: Some(process.id().clone()),
		remote: remote.cloned(),
		..Default::default()
	};
	let process = tg::Process::spawn(runtime.server(), arg).await?;
	let output = process.output(runtime.server()).await?;
	let output = output
		.try_unwrap_string()
		.map_err(|source| tg::error!(!source, "expected a string"))?;
	let checksum = output.parse()?;

	Ok(checksum)
}

pub async fn stdio_task(
	server: &Server,
	process: &tg::Process,
	stop: Stop,
	stdin: Option<sandbox::Stdin>,
	stdout: Option<sandbox::Stdout>,
	stderr: Option<sandbox::Stderr>,
) -> tg::Result<()> {
	let state = process.load(server).await?;

	let stdin = tokio::spawn({
		let server = server.clone();
		let io = state.stdin.clone();
		let blob = state.command.load(&server).await?.stdin.clone();
		let remote = process.remote().cloned();

		async move {
			let Some(mut stdin) = stdin else {
				return;
			};
			if let Some(io) = io {
				// Write stdin from pipe/pty.
				input(&server, &io, remote, &mut stdin, stop)
					.await
					.inspect_err(|source| tracing::error!(?source, "failed to write stdin"))
					.ok();
			} else if let Some(blob) = blob {
				// Copy blobs to stdin.
				let Ok(mut reader) = blob
					.read(&server, tg::blob::read::Arg::default())
					.await
					.inspect_err(|error| tracing::error!(?error, "failed to read blob"))
				else {
					return;
				};
				tokio::io::copy(&mut reader, &mut stdin)
					.await
					.inspect_err(|error| tracing::error!(?error, "failed to copy blob"))
					.ok();
			}

			// Shutdown stdin to make sure the process exits if it's reading from stdin.
			stdin.shutdown().await.ok();
		}
	});

	let stdout = tokio::spawn({
		let server = server.clone();
		let process = process.clone();
		async move {
			let Some(stdout) = stdout else { return Ok(()) };
			output(&server, &process, tg::process::log::Stream::Stdout, stdout)
				.await
				.inspect_err(|source| tracing::error!(?source, "failed to read stdout"))
		}
	});

	let stderr = tokio::spawn({
		let server = server.clone();
		let process = process.clone();
		async move {
			let Some(stderr) = stderr else { return Ok(()) };
			output(&server, &process, tg::process::log::Stream::Stderr, stderr)
				.await
				.inspect_err(|source| tracing::error!(?source, "failed to read stderr"))
		}
	});

	// Join the tasks.
	let (stdout, stderr) = future::join(stdout, stderr).await;
	stdin.abort();
	stdout
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to read stdout from pipe"))?;
	stderr
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to read stderr from pipe"))?;

	Ok::<_, tg::Error>(())
}

// Helper to create an event stream from pipes
fn chunk_stream_from_reader(
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> impl Stream<Item = tg::Result<Bytes>> + Send + 'static {
	let buffer = vec![0u8; 4096];
	stream::try_unfold((reader, buffer), |(mut reader, mut buffer)| async move {
		let size = reader
			.read(&mut buffer)
			.await
			.map_err(|source| tg::error!(!source, "failed to read"))?;
		if size == 0 {
			return Ok(None);
		}
		let chunk = Bytes::copy_from_slice(&buffer[0..size]);
		Ok(Some((chunk, (reader, buffer))))
	})
}

pub async fn signal_task(
	server: &Server,
	pid: libc::pid_t,
	process: &tg::Process,
) -> tg::Result<()> {
	// Get the signal stream for the process.
	let arg = tg::process::signal::get::Arg {
		remote: process.remote().cloned(),
	};
	let mut stream = server.try_get_process_signal_stream(process.id(), arg).await
		.map_err(|source| tg::error!(!source, %process = process.id(), "failed to get the process's signal stream"))?
		.ok_or_else(|| tg::error!(%process = process.id(), "expected the process's signal stream to exist"))?;

	// Handle the events.
	while let Some(event) = stream.try_next().await? {
		match event {
			tg::process::signal::get::Event::Signal(signal) => unsafe {
				let ret = libc::kill(pid, signal_number(signal));
				if ret != 0 {
					let error = std::io::Error::last_os_error();
					tracing::error!(?error, "failed to send the signal");
				}
			},
			tg::process::signal::get::Event::End => break,
		}
	}

	Ok(())
}

fn signal_number(signal: tg::process::Signal) -> i32 {
	match signal {
		tg::process::Signal::SIGABRT => libc::SIGABRT,
		tg::process::Signal::SIGFPE => libc::SIGFPE,
		tg::process::Signal::SIGILL => libc::SIGILL,
		tg::process::Signal::SIGALRM => libc::SIGALRM,
		tg::process::Signal::SIGHUP => libc::SIGHUP,
		tg::process::Signal::SIGINT => libc::SIGINT,
		tg::process::Signal::SIGKILL => libc::SIGKILL,
		tg::process::Signal::SIGPIPE => libc::SIGPIPE,
		tg::process::Signal::SIGQUIT => libc::SIGQUIT,
		tg::process::Signal::SIGSEGV => libc::SIGSEGV,
		tg::process::Signal::SIGTERM => libc::SIGTERM,
		tg::process::Signal::SIGUSR1 => libc::SIGUSR1,
		tg::process::Signal::SIGUSR2 => libc::SIGUSR2,
	}
}

async fn input(
	server: &Server,
	stdio: &tg::process::Stdio,
	remote: Option<String>,
	stdin: &mut sandbox::Stdin,
	stop: Stop,
) -> tg::Result<()> {
	match stdio {
		tg::process::Stdio::Pipe(id) => {
			let arg = tg::pipe::read::Arg { remote };
			let stream = server.read_pipe(id, arg).await?;
			let mut stream = pin!(stream);
			loop {
				let stop_ = stop.wait();
				let event = stream.try_next();
				let chunk = match future::select(event, pin!(stop_)).await {
					future::Either::Left((Ok(Some(tg::pipe::Event::Chunk(chunk))), _)) => chunk,
					future::Either::Left((Err(source), _)) => {
						return Err(source);
					},
					_ => break,
				};
				let stop_ = stop.wait();
				let write = stdin.write_all(&chunk);
				match future::select(pin!(write), pin!(stop_)).await {
					future::Either::Left((Ok(()), _)) => (),
					future::Either::Left((Err(ref source), _))
						if source.raw_os_error() == Some(libc::EPIPE) =>
					{
						break;
					},
					future::Either::Left((Err(source), _)) => {
						return Err(tg::error!(!source, "failed to write stdin"));
					},
					future::Either::Right(_) => {
						break;
					},
				}
			}
			stdin.shutdown().await.ok();
		},
		tg::process::Stdio::Pty(id) => {
			let arg = tg::pty::read::Arg {
				master: false,
				remote,
			};
			let stream = server.read_pty(id, arg).await?;
			let mut stream = pin!(stream);
			loop {
				let stop = stop.wait();
				let event = stream.try_next();
				match future::select(event, pin!(stop)).await {
					future::Either::Left((Ok(Some(tg::pty::Event::Chunk(chunk))), _)) => {
						stdin
							.write_all(&chunk)
							.await
							.map_err(|source| tg::error!(!source, "failed to write bytes"))?;
					},
					future::Either::Left((Ok(Some(tg::pty::Event::Size(size))), _)) => {
						let tty = sandbox::Tty {
							cols: size.cols,
							rows: size.rows,
						};
						stdin.change_window_size(tty).await.map_err(|source| {
							tg::error!(!source, "failed to change the PTY window size")
						})?;
					},
					future::Either::Left((Err(source), _)) => {
						return Err(source);
					},
					_ => break,
				}
			}
		},
	}
	Ok(())
}

async fn output(
	server: &Server,
	process: &tg::Process,
	stream: tg::process::log::Stream,
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	let chunks = chunk_stream_from_reader(reader);
	let mut chunks = pin!(chunks);
	while let Some(bytes) = chunks.try_next().await? {
		let arg = tg::process::log::post::Arg {
			bytes,
			stream,
			remote: process.remote().cloned(),
		};
		server.try_post_process_log(process.id(), arg).await?;
	}
	Ok(())
}

pub async fn which(exe: &Path, env: &BTreeMap<String, String>) -> tg::Result<String> {
	if exe.is_absolute() || exe.components().count() > 1 {
		return Ok(exe.to_string_lossy().to_string());
	}
	let Some(pathenv) = env.get("PATH") else {
		return Ok(exe.to_string_lossy().to_string());
	};
	let name = exe.components().next();
	let Some(std::path::Component::Normal(name)) = name else {
		return Err(tg::error!(%path = exe.display(), "invalid executable path"));
	};
	let sep = ":";
	for path in pathenv.split(sep) {
		let path = Path::new(path).join(name);
		if tokio::fs::try_exists(&path).await.ok() == Some(true) {
			return Ok(path.to_string_lossy().to_string());
		}
	}
	Err(tg::error!(%path = exe.display(), "could not find executable"))
}

pub async fn log(
	server: &Server,
	process: &tg::Process,
	stream: tg::process::log::Stream,
	message: String,
) {
	let is_pty = match stream {
		tg::process::log::Stream::Stderr => {
			matches!(
				process.load(server).await.unwrap().stderr,
				Some(tg::process::Stdio::Pty(_))
			)
		},
		tg::process::log::Stream::Stdout => {
			matches!(
				process.load(server).await.unwrap().stdout,
				Some(tg::process::Stdio::Pty(_))
			)
		},
	};
	let message = if is_pty {
		message.replace('\n', "\r\n")
	} else {
		message
	};
	let arg = tg::process::log::post::Arg {
		bytes: message.into(),
		remote: process.remote().cloned(),
		stream,
	};
	server
		.try_post_process_log(process.id(), arg)
		.await
		.inspect_err(|error| tracing::error!(?error, "failed to post process log"))
		.ok();
}
