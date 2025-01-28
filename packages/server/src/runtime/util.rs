use super::Runtime;
use crate::Server;
use bytes::Bytes;
use std::{path::Path, pin::pin};
use tangram_client::{self as tg, handle::Ext as _};
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
	process: &tg::process::Id,
	remote: Option<&String>,
	reader: impl AsyncRead + Send + 'static,
) -> tokio::task::JoinHandle<tg::Result<()>> {
	let server = server.clone();
	let process = tg::Process::with_id(process.clone());
	let remote = remote.cloned();
	tokio::spawn(async move {
		let mut reader = pin!(reader);
		let mut buffer = vec![0; 4096];
		loop {
			let size = reader
				.read(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read from the log"))?;
			if size == 0 {
				return Ok::<_, tg::Error>(());
			}
			let bytes = Bytes::copy_from_slice(&buffer[0..size]);
			if server.config.advanced.write_process_logs_to_stderr {
				tokio::io::stderr()
					.write_all(&bytes)
					.await
					.inspect_err(|error| {
						tracing::error!(?error, "failed to write the build log to stderr");
					})
					.ok();
			}
			let arg = tg::process::log::post::Arg {
				bytes,
				remote: remote.clone(),
			};
			process.post_log(&server, arg).await?;
		}
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

	// checksum = unsafe implies there is no checksum, so return Ok
	if algorithm == tg::checksum::Algorithm::Unsafe {
		return Ok(());
	}

	// Create the checksum command.
	let host = "builtin";
	let args = vec![
		"checksum".into(),
		value.clone(),
		algorithm.to_string().into(),
	];
	let command = tg::Command::builder(host).args(args).build();
	let command_id = command.id(runtime.server()).await?;
	let arg = tg::command::spawn::Arg {
		create: true,
		parent: Some(process.clone()),
		..Default::default()
	};

	// Spawn the checksum process.
	let process = runtime
		.server()
		.spawn_command(&command_id, arg)
		.await?
		.process;
	let output = tg::Process::with_id(process).wait(runtime.server()).await?;
	if output.status.is_succeeded() {
		Ok(())
	} else {
		Err(tg::error!("the checksum process failed"))
	}
}
