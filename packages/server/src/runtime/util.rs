use crate::Server;
use std::path::Path;
use tangram_client::{self as tg, handle::Ext as _};

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

pub async fn find_matching_build(
	server: &Server,
	target: &tg::Target,
) -> tg::Result<Option<tg::build::Id>> {
	let target = target.load(server).await?;
	let search_target = tg::target::Builder::from(&*target)
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

	let search_target = tg::target::Builder::from(&*target)
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
	server.get_build_outcome(&output.build).await?.await?;
	Ok(())
}
