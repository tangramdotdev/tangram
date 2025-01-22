use crate::Server;
use bytes::Bytes;
use futures::TryStreamExt as _;
use std::{path::Path, pin::pin};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::Runtime;

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

impl Runtime {
	pub async fn try_reuse_process(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
	) -> tg::Result<Option<super::Output>> {
		// Get the checksum if set.
		let Some(checksum) = command.checksum(self.server()).await?.clone() else {
			return Ok(None);
		};

		// Break if the checksum type is none or unsafe.
		if let tg::Checksum::None | tg::Checksum::Unsafe = &checksum {
			return Ok(None);
		}

		// Search for an existing process with the checksum none or unsafe.
		let Ok(Some(matching_process)) = self.find_matching_process(command).await else {
			return Err(tg::error!("failed to find a matching process"));
		};

		// Wait for the process to finish.
		let Some(stream) = self
			.server()
			.try_get_process_status(&matching_process)
			.await?
		else {
			return Err(tg::error!("failed to get the process status stream"));
		};
		let Some(Ok(_)) = pin!(stream).last().await else {
			return Err(
				tg::error!(%process = matching_process, "failed to wait for the original process"),
			)?;
		};

		// Get the original process.
		let (error, exit, value) = self
			.server()
			.get_process(&matching_process)
			.await
			.map(|output| (output.error, output.exit, output.output))?;

		// Return early if the original process has no output value.
		let Some(value) = value else {
			return Ok(None);
		};

		// Get the output value.
		let value: tg::Value = value
			.try_into()
			.map_err(|_| tg::error!("failed to get matching process output"))?;

		// Validate the checksum of the value.
		self.checksum(process, &value, &checksum).await?;

		// Copy the process children and log.
		self.copy_process_children_and_log(&matching_process, process)
			.await?;

		// Return the output.
		let output = super::Output {
			error,
			exit,
			value: Some(value),
		};
		Ok(Some(output))
	}

	async fn find_matching_process(
		&self,
		command: &tg::Command,
	) -> tg::Result<Option<tg::process::Id>> {
		// Attempt to find a process with the checksum set to none.
		let command = command.load(self.server()).await?;
		let search_target = tg::command::Builder::with_object(&command)
			.checksum(tg::Checksum::None)
			.build();
		let target_id = search_target.id(self.server()).await?;
		let arg = tg::command::spawn::Arg {
			create: false,
			..Default::default()
		};
		if let Some(output) = self.server().try_spawn_command(&target_id, arg).await? {
			return Ok(Some(output.process));
		}

		// Attempt to find a process with the checksum set to unsafe.
		let search_command = tg::command::Builder::with_object(&command)
			.checksum(tg::Checksum::Unsafe)
			.build();
		let target_id = search_command.id(self.server()).await?;
		let arg = tg::command::spawn::Arg {
			create: false,
			..Default::default()
		};
		if let Some(output) = self.server().try_spawn_command(&target_id, arg).await? {
			return Ok(Some(output.process));
		}

		Ok(None)
	}

	pub(super) async fn checksum(
		&self,
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
		let command_id = command.id(self.server()).await?;
		let arg = tg::command::spawn::Arg {
			create: true,
			parent: Some(process.clone()),
			..Default::default()
		};

		// Spawn the checksum process.
		let process = self.server().spawn_command(&command_id, arg).await?.process;
		let Some(stream) = self.server().try_get_process_status(&process).await? else {
			return Err(tg::error!("failed to get the process status"));
		};
		let Some(Ok(status)) = pin!(stream).last().await else {
			return Err(tg::error!("failed to get the last process status"));
		};
		if status.is_succeeded() {
			Ok(())
		} else {
			Err(tg::error!("the checksum process failed"))
		}
	}

	async fn copy_process_children_and_log(
		&self,
		src_process: &tg::process::Id,
		dst_process: &tg::process::Id,
	) -> tg::Result<()> {
		// Copy the children.
		let arg = tg::process::children::get::Arg::default();
		let mut src_children = pin!(self.server().get_process_children(src_process, arg).await?);
		while let Some(chunk) = src_children.try_next().await? {
			for child in chunk.data {
				if !self
					.server()
					.try_add_process_child(dst_process, &child)
					.await?
				{
					break;
				}
			}
		}

		// Copy the log.
		let arg = tg::process::log::get::Arg::default();
		let mut src_log = pin!(self.server().get_process_log(src_process, arg).await?);
		while let Some(chunk) = src_log.try_next().await? {
			let arg = tg::process::log::post::Arg {
				bytes: chunk.bytes,
				remote: None,
			};
			self.server().try_post_process_log(dst_process, arg).await?;
		}

		Ok(())
	}
}
