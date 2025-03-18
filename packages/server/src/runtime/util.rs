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
	stdin: sandbox::Stdin,
	stdout: sandbox::Stdout,
	stderr: sandbox::Stderr,
) -> tg::Result<()> {
	let state = process.load(&server).await?;
	if state.cacheable {
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
			while let Some(tg::pty::Event::Chunk(bytes)) = stream.try_next().await? {
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
			while let Some(tg::pty::Event::Chunk(bytes)) = stream.try_next().await? {
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
		let state = process.load(&server).await?;
		let process = process.clone();
		async move {
			let Some(pipe) = state.stdin.as_ref() else {
				return Ok(());
			};
			let arg = tg::pty::get::Arg {
				remote: process.remote().cloned(),
			};

			let stream = server.get_pipe_stream(pipe, arg).await?;
			let mut stream = pin!(stream);
			let mut stdin = pin!(stdin);
			loop {
				let stop = pin!(stop.wait());
				let next = pin!(stream.try_next());
				let fut = future::select(stop, next);
				match fut.await {
					future::Either::Left(_) => break,
					future::Either::Right((Ok(Some(event)), _)) => match event {
						tg::pty::Event::Chunk(chunk) => {
							stdin.write_all(&chunk).await.map_err(
								|source| tg::error!(!source, %pipe, "failed to write stdin"),
							)?;
						},
						tg::pty::Event::WindowSize(window_size) => {
							let tty = sandbox::Tty {
								rows: window_size.rows,
								cols: window_size.cols,
								x: window_size.xpos,
								y: window_size.ypos,
							};
							stdin.change_window_size(tty).await.map_err(|source| {
								tg::error!(!source, "failed to set window size")
							})?;
						},
						tg::pty::Event::End => break,
					},
					future::Either::Right(_) => break,
				}
			}
			stdin.shutdown().await.ok();

			Ok::<_, tg::Error>(())
		}
	});

	// Create a task for stdout.
	let stdout = tokio::spawn({
		let server = server.clone();
		let state = process.load(&server).await?;
		let process = process.clone();
		async move {
			let Some(pipe) = state.stdout.as_ref() else {
				return Ok(());
			};
			let arg = tg::pty::post::Arg {
				remote: process.remote().cloned(),
			};
			let stream = chunk_stream_from_reader(stdout);
			server.post_pipe(pipe, arg, stream).await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Create a task for stderr.
	let stderr = tokio::spawn({
		let server = server.clone();
		let state = process.load(&server).await?;
		let process = process.clone();
		async move {
			let Some(pipe) = state.stderr.as_ref() else {
				return Ok(());
			};
			let arg = tg::pty::post::Arg {
				remote: process.remote().cloned(),
			};
			let stream = chunk_stream_from_reader(stderr);
			server.post_pipe(pipe, arg, stream).await?;
			Ok::<_, tg::Error>(())
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
) -> impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static {
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
		let event = tg::pty::Event::Chunk(chunk);
		Ok(Some((event, (reader, buffer))))
	})
	.chain(stream::once(future::ok(tg::pty::Event::End)))
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

fn signal_number(signal: tg::process::Signal) -> i32 {
	// Convert a tg::process::Signal to an OS signal.
	match signal {
		tg::process::Signal::SIGABRT => libc::SIGABRT,
		tg::process::Signal::SIGFPE => libc::SIGFPE,
		tg::process::Signal::SIGILL => libc::SIGILL,
		tg::process::Signal::SIGALRM => libc::SIGALRM,
		tg::process::Signal::SIGHUP => libc::SIGHUP,
		tg::process::Signal::SIGINT => libc::SIGABRT,
		tg::process::Signal::SIGKILL => libc::SIGKILL,
		tg::process::Signal::SIGPIPE => libc::SIGPIPE,
		tg::process::Signal::SIGQUIT => libc::SIGQUIT,
		tg::process::Signal::SIGSEGV => libc::SIGSEGV,
		tg::process::Signal::SIGTERM => libc::SIGTERM,
		tg::process::Signal::SIGUSR1 => libc::SIGUSR1,
		tg::process::Signal::SIGUSR2 => libc::SIGUSR2,
	}
}
