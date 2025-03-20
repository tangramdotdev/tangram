use super::Runtime;
use crate::Server;
use bytes::Bytes;
use futures::{
	Stream, StreamExt as _, TryStreamExt as _, future,
	stream::{self, FuturesOrdered},
};
use std::{collections::BTreeMap, path::Path, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Stop;
use tangram_sandbox as sandbox;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};

/// Render a value.
pub async fn render(
	server: &Server,
	value: &tg::Value,
	artifacts_path: &Path,
) -> tg::Result<String> {
	if let Ok(string) = value.try_unwrap_string_ref() {
		Ok(string.clone())
	} else if let Ok(artifact) = tg::Artifact::try_from(value.clone()) {
		Ok(artifacts_path
			.join(artifact.id(server).await?.to_string())
			.into_os_string()
			.into_string()
			.unwrap())
	} else if let Ok(template) = value.try_unwrap_template_ref() {
		return template
			.try_render(|component| async move {
				match component {
					tg::template::Component::String(string) => Ok(string.clone()),
					tg::template::Component::Artifact(artifact) => Ok(artifacts_path
						.join(artifact.id(server).await?.to_string())
						.into_os_string()
						.into_string()
						.unwrap()),
				}
			})
			.await;
	} else {
		Ok("<tangram value>".to_owned())
	}
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
	let args = vec![
		"checksum".into(),
		value.clone(),
		algorithm.to_string().into(),
	];
	let command = tg::Command::builder(host).args(args).build();

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
						let arg = tg::process::log::post::Arg {
							bytes: message.into(),
							remote: Some(remote.to_owned()),
						};
						runtime
							.server()
							.try_post_process_log(process.id(), arg)
							.await
							.ok();
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
		create: true,
		parent: Some(process.id().clone()),
		remote: remote.cloned(),
		..Default::default()
	};
	let output = tg::Process::run(runtime.server(), arg).await?;
	let output = output
		.try_unwrap_string()
		.map_err(|source| tg::error!(!source, "expected a string"))?;

	output.parse()
}

pub async fn merge_env(
	server: &Server,
	artifacts_path: &Path,
	process: Option<&BTreeMap<String, String>>,
	command: &tg::value::Map,
) -> tg::Result<BTreeMap<String, String>> {
	let mut env = process
		.iter()
		.flat_map(|env| env.iter())
		.map(|(key, value)| (key.to_owned(), tg::Value::String(value.clone())))
		.collect::<tg::value::Map>();

	for (key, value) in command {
		let mutation = match value {
			tg::Value::Mutation(value) => value.clone(),
			value => tg::Mutation::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(key, &mut env)?;
	}

	env.iter()
		.map(|(key, value)| async {
			let key = key.clone();
			let value = render(server, value, artifacts_path).await?;
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<FuturesOrdered<_>>()
		.try_collect()
		.await
}

pub async fn stdio_task(
	server: Server,
	process: tg::Process,
	stop: Stop,
	mut stdin: sandbox::Stdin,
	stdout: sandbox::Stdout,
	stderr: sandbox::Stderr,
) -> tg::Result<()> {
	let state = process.load(&server).await?;
	if state.cacheable {
		stdin.shutdown().await.ok();
		log_task(&server, &process, stdout, stderr).await?;
	} else {
		pipe_task(&server, &process, stop, stdin, stdout, stderr).await?;
	};
	Ok(())
}

async fn log_task(
	server: &Server,
	process: &tg::Process,
	stdout: impl AsyncRead + Unpin + Send + 'static,
	stderr: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	// Create a task for stdout.
	let stdout = tokio::spawn({
		let server = server.clone();
		let process = process.clone();
		async move {
			let stream = chunk_stream_from_reader(stdout);
			let mut stream = pin!(stream);
			while let Some(bytes) = stream.try_next().await? {
				if server.config().advanced.write_process_logs_to_stderr {
					tokio::io::stderr().write_all(&bytes).await.ok();
				}
				let arg = tg::process::log::post::Arg {
					bytes,
					remote: process.remote().cloned(),
				};
				server.try_post_process_log(process.id(), arg).await?;
			}
			Ok::<_, tg::Error>(())
		}
	});

	// Create a task for stderr
	let stderr = tokio::spawn({
		let server = server.clone();
		let process = process.clone();
		async move {
			let stream = chunk_stream_from_reader(stderr);
			let mut stream = pin!(stream);
			while let Some(bytes) = stream.try_next().await? {
				if server.config().advanced.write_process_logs_to_stderr {
					tokio::io::stderr().write_all(&bytes).await.ok();
				}
				let arg = tg::process::log::post::Arg {
					bytes,
					remote: process.remote().cloned(),
				};
				server.try_post_process_log(process.id(), arg).await?;
			}
			Ok::<_, tg::Error>(())
		}
	});

	let (stdout, stderr) = future::join(stdout, stderr).await;
	stdout
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to write stdout to log"))?;
	stderr
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to write stderr to log"))?;

	Ok(())
}

async fn pipe_task(
	server: &Server,
	process: &tg::Process,
	stop: Stop,
	stdin: sandbox::Stdin,
	stdout: sandbox::Stdout,
	stderr: sandbox::Stderr,
) -> tg::Result<()> {
	// Create a task for stdin.
	let stdin = tokio::spawn({
		let server = server.clone();
		let io = process.load(&server).await?.stdin.clone();
		let remote = process.remote().cloned();
		async move {
			let Some(io) = io.as_ref() else {
				return;
			};
			input(&server, io, remote, stdin, stop)
				.await
				.inspect_err(|source| tracing::error!(?source, "failed to write stdin"))
				.ok();
		}
	});

	let stdout = tokio::spawn({
		let server = server.clone();
		let io = process.load(&server).await?.stdout.clone();
		let remote = process.remote().cloned();
		async move {
			let Some(io) = io.as_ref() else {
				return Ok(());
			};
			output(&server, io, remote, stdout)
				.await
				.inspect_err(|source| tracing::error!(?source, "failed to read stdout"))
		}
	});

	let stderr = tokio::spawn({
		let server = server.clone();
		let io = process.load(&server).await?.stderr.clone();
		let remote = process.remote().cloned();
		async move {
			let Some(io) = io.as_ref() else {
				return Ok(());
			};
			output(&server, io, remote, stderr)
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
		.ok_or_else(|| tg::error!(%process = process.id(), "the process was destroyed before it could be waited"))?;

	while let Some(event) = stream.try_next().await? {
		match event {
			tg::process::signal::get::Event::Signal(signal) => unsafe {
				if libc::kill(pid, signal_number(signal)) != 0 {
					let error = std::io::Error::last_os_error();
					tracing::error!(?error, "failed to send signal");
				}
			},
			tg::process::signal::get::Event::End => break,
		}
	}
	Ok(())
}

pub async fn write_io_bytes(
	server: &Server,
	io: &tg::process::Io,
	remote: Option<String>,
	bytes: Bytes,
) -> tg::Result<()> {
	match io {
		tg::process::Io::Pipe(id) => server.write_pipe_bytes(id, remote, bytes).await,
		tg::process::Io::Pty(id) => server.write_pty_bytes(id, remote, bytes).await,
	}
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
	io: &tg::process::Io,
	remote: Option<String>,
	mut stdin: sandbox::Stdin,
	stop: Stop,
) -> tg::Result<()> {
	match io {
		tg::process::Io::Pipe(id) => {
			let arg = tg::pipe::get::Arg { remote };
			let stream = server.get_pipe_stream(id, arg).await?;
			let mut stream = pin!(stream);
			loop {
				let stop = stop.wait();
				let event = stream.try_next();
				match future::select(event, pin!(stop)).await {
					future::Either::Left((Ok(Some(tg::pipe::Event::Chunk(chunk))), _)) => {
						stdin
							.write_all(&chunk)
							.await
							.map_err(|source| tg::error!(!source, "failed to write bytes"))?;
					},
					future::Either::Left((Err(source), _)) => {
						return Err(source);
					},
					_ => break,
				}
			}
		},
		tg::process::Io::Pty(id) => {
			let arg = tg::pty::get::Arg {
				master: true,
				remote,
			};
			let stream = server.get_pty_stream(id, arg).await?;
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
					future::Either::Left((Ok(Some(tg::pty::Event::WindowSize(ws))), _)) => {
						let tty = sandbox::Tty {
							cols: ws.cols,
							rows: ws.rows,
							x: ws.xpos,
							y: ws.ypos,
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
	io: &tg::process::Io,
	remote: Option<String>,
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	let stream = chunk_stream_from_reader(reader);
	match io {
		tg::process::Io::Pipe(id) => {
			let stream = stream
				.map_ok(tg::pipe::Event::Chunk)
				.chain(stream::once(future::ok(tg::pipe::Event::End)))
				.boxed();
			let arg = tg::pipe::post::Arg { remote };
			server.post_pipe(id, arg, stream).await?;
		},
		tg::process::Io::Pty(id) => {
			let stream = stream
				.map_ok(tg::pty::Event::Chunk)
				.chain(stream::once(future::ok(tg::pty::Event::End)))
				.boxed();
			let arg = tg::pty::post::Arg {
				remote,
				master: false,
			};
			server.post_pty(id, arg, stream).await?;
		},
	}
	Ok(())
}
