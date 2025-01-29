use super::Runtime;
use crate::Server;
use bytes::Bytes;
use std::{path::Path, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::Ext as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

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

// Post process logs.
pub fn post_log_task(
	server: &Server,
	process: &tg::process::get::Output,
	remote: Option<&String>,
	stdout: impl AsyncRead + Send + 'static,
	stderr: impl AsyncRead + Send + 'static,
) -> tokio::task::JoinHandle<tg::Result<()>> {
	async fn inner(
		server: Server,
		process: tg::process::Id,
		remote: Option<String>,
		reader: impl AsyncRead + Send + 'static,
		pipe: Option<tg::pipe::Id>,
	) -> tg::Result<()> {
		let process = tg::Process::with_id(process);
		let mut reader = pin!(reader);
		let mut buffer = vec![0; 4096];
		loop {
			// Read from the reader.
			let size = reader
				.read(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read from the log"))?;
			if size == 0 {
				return Ok::<_, tg::Error>(());
			}
			let bytes = Bytes::copy_from_slice(&buffer[0..size]);

			// Write to stderr if configured.
			if server.config.advanced.write_process_logs_to_stderr {
				tokio::io::stderr()
					.write_all(&bytes)
					.await
					.inspect_err(|error| {
						tracing::error!(?error, "failed to write the build log to stderr");
					})
					.ok();
			}

			// Write to the pipe.
			if let Some(pipe) = &pipe {
				server.write_pipe_chunk(pipe, bytes.clone()).await.ok();
			}

			// Write the log.
			let arg = tg::process::log::post::Arg {
				bytes: bytes.clone(),
				remote: remote.clone(),
			};
			process.post_log(&server, arg).await?;
		}
	}

	// Create the futures for stdout/stderr readers.
	let stdout = inner(
		server.clone(),
		process.id.clone(),
		remote.cloned(),
		stdout,
		process.stdout.clone(),
	);
	let stderr = inner(
		server.clone(),
		process.id.clone(),
		remote.cloned(),
		stderr,
		process.stderr.clone(),
	);

	// Spawn the task
	tokio::spawn(async move {
		futures::try_join!(stderr, stdout)?;
		Ok(())
	})
}

pub async fn compute_checksum(
	runtime: &Runtime,
	process: &tg::process::Id,
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
		parent: Some(process.clone()),
		..Default::default()
	};
	tg::Process::build(runtime.server(), arg).await?;

	Ok(())
}
