use super::Runtime;
use crate::Server;
use bytes::Bytes;
use futures::{
	future,
	stream::{self, FuturesOrdered},
	Stream, StreamExt, TryStreamExt,
};
use std::{collections::BTreeMap, path::Path, pin::pin};
use tangram_client as tg;
use tangram_futures::task::{Stop, Task};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio_stream::wrappers::ReceiverStream;

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
) -> tg::Result<()> {
	let algorithm = checksum.algorithm();
	let algorithm = if algorithm == tg::checksum::Algorithm::None {
		tg::checksum::Algorithm::Sha256
	} else {
		algorithm
	};

	if algorithm == tg::checksum::Algorithm::Any {
		return Ok(());
	}

	let host = "builtin";
	let args = vec![
		"checksum".into(),
		value.clone(),
		algorithm.to_string().into(),
	];
	let command = tg::Command::builder(host).args(args).build();
	let arg = tg::process::spawn::Arg {
		command: Some(command.id(runtime.server()).await?),
		create: true,
		parent: Some(process.id().clone()),
		..Default::default()
	};
	tg::Process::run(runtime.server(), arg).await?;

	Ok(())
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
	stdin: impl AsyncWrite + Send + 'static,
	stdout: impl AsyncRead + Unpin + Send + 'static,
	stderr: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	let state = process.load(&server).await?;
	if state.cacheable {
		log_task(&server, &process, stop, stdout, stderr).await?;
	} else {
		pipe_task(&server, &process, stop, stdin, stdout, stderr).await?;
	};
	Ok(())
}

async fn log_task(
	server: &Server,
	process: &tg::Process,
	_stop: Stop,
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
	stdin: impl AsyncWrite + Send + 'static,
	stdout: impl AsyncRead + Unpin + Send + 'static,
	stderr: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	// Create a task for stdin.
	let stdin = tokio::spawn({
		let server = server.clone();
		let state = process.load(&server).await?;
		async move {
			let Some(pipe) = state.stdin.as_ref() else {
				return Ok(());
			};
			let stream = server.read_pipe(pipe).await?;
			let mut stream = pin!(stream);
			let mut stdin = pin!(stdin);
			loop {
				let event = stream.try_next();
				let wait = stop.wait();
				let chunk = match future::select(event, pin!(wait)).await {
					future::Either::Left((Ok(Some(tg::pipe::Event::Chunk(chunk))), ..)) => chunk,
					_ => break,
				};
				if let Err(error) = stdin.write_all(&chunk).await {
					tracing::error!(%pipe, %error, "failed to write stdin");
					break;
				}
			}
			server.close_pipe(pipe).await.ok();
			eprintln!("stdin task done");
			Ok::<_, tg::Error>(())
		}
	});

	// Create a task for stdout.
	let stdout = tokio::spawn({
		let server = server.clone();
		let state = process.load(&server).await?;
		async move {
			let Some(pipe) = state.stdout.as_ref() else {
				return Ok(());
			};
			let stream = chunk_stream_from_reader(stdout);
			server.write_pipe(pipe, stream).await?;
			eprintln!("stdout task done");
			Ok::<_, tg::Error>(())
		}
	});

	// Create a task for stderr.
	let stderr = tokio::spawn({
		let server = server.clone();
		let state = process.load(&server).await?;
		async move {
			let Some(pipe) = state.stderr.as_ref() else {
				return Ok(());
			};
			let stream = chunk_stream_from_reader(stderr);
			server.write_pipe(pipe, stream).await?;
			eprintln!("stderr task done");
			Ok::<_, tg::Error>(())
		}
	});

	// Join the tasks.
	let (stdin, stdout, stderr) = future::join3(stdin, stdout, stderr).await;
	stdin
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to write stdin to pipe"))?;
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
	// let reader = pin!(reader);
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
