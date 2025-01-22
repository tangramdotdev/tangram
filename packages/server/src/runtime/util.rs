use crate::Server;
use futures::{FutureExt as _, TryStreamExt as _};
use std::{path::Path, pin::pin};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;

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

pub async fn try_reuse_process(
	server: &Server,
	process: &tg::process::Id,
	command: &tg::Command,
	checksum: Option<&tg::Checksum>,
) -> tg::Result<tg::Value> {
	// Unwrap the checksum.
	let Some(checksum) = checksum else {
		return Err(tg::error!("failed to get the checksum"));
	};

	// Break if the checksum type is none or unsafe.
	if let tg::Checksum::None | tg::Checksum::Unsafe = &checksum {
		return Err(tg::error!("inappropriate checksum type"));
	}

	// Search for an existing process with the checksum none or unsafe.
	let Ok(Some(matching_process)) = find_matching_process(server, command).await else {
		return Err(tg::error!("failed to find a matching process"));
	};

	// Wait for the process to finish and get its output.
	let output = tg::Process::with_id(matching_process.clone())
		.output(server)
		.boxed()
		.await?;

	// Checksum the output.
	super::util::checksum(server, &matching_process, &output, checksum)
		.boxed()
		.await?;

	// Copy the process children and log.
	copy_process_children_and_log(server, &matching_process, process).await?;

	Ok(output)
}

async fn find_matching_process(
	server: &Server,
	command: &tg::Command,
) -> tg::Result<Option<tg::process::Id>> {
	// Attempt to find a process with the checksum set to none.
	let command = command.load(server).await?;
	let search_target = tg::command::Builder::with_object(&command)
		.checksum(tg::Checksum::None)
		.build();
	let target_id = search_target.id(server).await?;
	let arg = tg::command::spawn::Arg {
		create: false,
		..Default::default()
	};
	if let Some(output) = server.try_spawn_command(&target_id, arg).await? {
		return Ok(Some(output.process));
	}

	// Attempt to find a process with the checksum set to unsafe.
	let search_command = tg::command::Builder::with_object(&command)
		.checksum(tg::Checksum::Unsafe)
		.build();
	let target_id = search_command.id(server).await?;
	let arg = tg::command::spawn::Arg {
		create: false,
		..Default::default()
	};
	if let Some(output) = server.try_spawn_command(&target_id, arg).await? {
		return Ok(Some(output.process));
	}

	Ok(None)
}

async fn copy_process_children_and_log(
	server: &Server,
	src_process: &tg::process::Id,
	dst_process: &tg::process::Id,
) -> tg::Result<()> {
	// Copy the children.
	let arg = tg::process::children::get::Arg::default();
	let mut src_children = pin!(server.get_process_children(src_process, arg).await?);
	while let Some(chunk) = src_children.try_next().await? {
		for child in chunk.data {
			if !server.try_add_process_child(dst_process, &child).await? {
				break;
			}
		}
	}

	// Copy the log.
	let arg = tg::process::log::get::Arg::default();
	let mut src_log = pin!(server.get_process_log(src_process, arg).await?);
	while let Some(chunk) = src_log.try_next().await? {
		let arg = tg::process::log::post::Arg {
			bytes: chunk.bytes,
			remote: None,
		};
		server.try_post_process_log(dst_process, arg).await?;
	}

	Ok(())
}

pub async fn checksum(
	server: &Server,
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
	if algorithm == tg::checksum::Algorithm::Unsafe {
		return Ok(());
	}
	let host = "builtin";
	let args = vec![
		"checksum".into(),
		value.clone(),
		algorithm.to_string().into(),
	];
	let command = tg::Command::builder(host).args(args).build();
	let command_id = command.id(server).await?;
	let arg = tg::command::spawn::Arg {
		create: true,
		parent: Some(process.clone()),
		..Default::default()
	};
	let output = server.spawn_command(&command_id, arg).await?;
	let Some(stream) = server.try_get_process_status(&output.process).await? else {
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
