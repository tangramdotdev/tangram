use super::{stdio, Runtime};
use crate::Server;
use bytes::Bytes;
use futures::{
	future,
	stream::{self, FuturesOrdered},
	Stream, StreamExt as _, TryStreamExt as _,
};
use std::{collections::BTreeMap, path::Path, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
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
	stdin: stdio::Host,
	stdout: stdio::Host,
	stderr: stdio::Host,
) -> tg::Result<()> {
	let state = process.load(&server).await?;
	if state.cacheable {
		log_task(&server, &process, stdout, stderr).await?;
	} else {
		pipe_task(&server, &process, stdin, stdout, stderr).await?;
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
			while let Some(tg::pipe::Event::Chunk(bytes)) = stream.try_next().await? {
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
			while let Some(tg::pipe::Event::Chunk(bytes)) = stream.try_next().await? {
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
	stdin: stdio::Host,
	stdout: stdio::Host,
	stderr: stdio::Host,
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
			let arg = tg::pipe::get::Arg {
				remote: process.remote().cloned(),
			};
			let stream = server.get_pipe_stream(pipe, arg).await?;
			let mut stream = pin!(stream);
			let mut stdin = pin!(stdin);
			while let Some(event) = stream.try_next().await? {
				match event {
					tg::pipe::Event::Chunk(chunk) => {
						stdin.write_all(&chunk).await.map_err(
							|source| tg::error!(!source, %pipe, "failed to write stdin"),
						)?;
					},
					tg::pipe::Event::WindowSize(window_size) => {
						stdin
							.set_window_size(window_size)
							.map_err(|source| tg::error!(!source, "failed to set window size"))?;
					},
					tg::pipe::Event::End => break,
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
			let arg = tg::pipe::post::Arg {
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
			let arg = tg::pipe::post::Arg {
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
) -> impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static {
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
		let event = tg::pipe::Event::Chunk(chunk);
		Ok(Some((event, (reader, buffer))))
	})
	.chain(stream::once(future::ok(tg::pipe::Event::End)))
}
