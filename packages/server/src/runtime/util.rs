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

pub async fn try_reuse_build(
	server: &Server,
	build: &tg::build::Id,
	target: &tg::Target,
	checksum: Option<&tg::Checksum>,
) -> tg::Result<tg::Value> {
	// Unwrap the checksum.
	let Some(checksum) = checksum else {
		return Err(tg::error!("failed to get the checksum"));
	};

	// Break if the checksum type is `None` or `Unsafe`.
	if let tg::Checksum::None | tg::Checksum::Unsafe = &checksum {
		return Err(tg::error!("inappropriate checksum type"));
	}

	// Search for an existing build with a `None` or `Unsafe` checksum.
	let Ok(Some(matching_build)) = find_matching_build(server, target).await else {
		return Err(tg::error!("failed to find a matching build"));
	};
	let matching_build = tg::Build::with_id(matching_build);

	// Wait for the build to finish and get its output.
	let output = matching_build.output(server).await?;
	let Some(value) = output.output else {
		return Err(tg::error!("failed to get the output"));
	};

	let value = tg::Value::try_from(value)?;

	// Checksum the output.
	super::util::checksum(server, &matching_build, &value, checksum)
		.boxed()
		.await?;

	// Copy the build children and log.
	copy_build_children_and_log(server, matching_build.id(), build).await?;

	Ok(value)
}

async fn find_matching_build(
	server: &Server,
	target: &tg::Target,
) -> tg::Result<Option<tg::build::Id>> {
	// Attempt to find a build with the checksum set to `None`.
	let target = target.load(server).await?;
	let search_target = tg::target::Builder::with_object(&target)
		.checksum(tg::Checksum::None)
		.build();
	let target_id = search_target.id(server).await?;
	let arg = tg::target::build::Arg {
		create: false,
		..Default::default()
	};
	if let Some(output) = server.try_build_target(&target_id, arg).await? {
		return Ok(Some(output.build));
	}

	// Attempt to find a build with the checksum set to `Unsafe`.
	let search_target = tg::target::Builder::with_object(&target)
		.checksum(tg::Checksum::Unsafe)
		.build();
	let target_id = search_target.id(server).await?;
	let arg = tg::target::build::Arg {
		create: false,
		..Default::default()
	};
	if let Some(output) = server.try_build_target(&target_id, arg).await? {
		return Ok(Some(output.build));
	}

	Ok(None)
}

async fn copy_build_children_and_log(
	server: &Server,
	src_build: &tg::build::Id,
	dst_build: &tg::build::Id,
) -> tg::Result<()> {
	// Copy the children.
	let arg = tg::build::children::get::Arg::default();
	let mut src_children = pin!(server.get_build_children(src_build, arg).await?);
	while let Some(chunk) = src_children.try_next().await? {
		for child in chunk.data {
			server.add_build_child(dst_build, &child).await?;
		}
	}

	// Copy the log.
	let arg = tg::build::log::get::Arg::default();
	let mut src_log = pin!(server.get_build_log(src_build, arg).await?);
	while let Some(chunk) = src_log.try_next().await? {
		let arg = tg::build::log::post::Arg {
			bytes: chunk.bytes,
			remote: None,
		};
		server.add_build_log(dst_build, arg).await?;
	}

	Ok(())
}

pub async fn checksum(
	server: &Server,
	build: &tg::Build,
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
	let target = tg::Target::builder(host).args(args).build();
	let target_id = target.id(server).await?;
	let arg = tg::target::build::Arg {
		create: true,
		parent: Some(build.id().clone()),
		..Default::default()
	};
	let output = server.build_target(&target_id, arg).await?;
	let Some(stream) = server.try_get_build_status(&output.build).await? else {
		return Err(tg::error!("failed to get build status"));
	};
	let Some(Ok(status)) = pin!(stream).last().await else {
		return Err(tg::error!("failed to get the last build status"));
	};
	if status.is_succeeded() {
		Ok(())
	} else {
		Err(tg::error!("checksum build failed"))
	}
}
