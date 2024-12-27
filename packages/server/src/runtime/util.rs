use crate::Server;
use std::path::Path;
use tangram_client as tg;

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

pub async fn spawn_checksum_build(
	server: &Server,
	parent_build_id: tg::build::Id,
	value: &tg::Value,
	checksum: &tg::Checksum,
) -> tg::Result<()> {
	// Create a child build to calculate the checksum.
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
		parent: Some(parent_build_id),
		..Default::default()
	};
	if let Some(output) = server.try_build_target(&target_id, arg).await? {
		if let Some(future) = server.try_get_build_outcome_future(&output.build).await? {
			future
				.await?
				.map(|_| ())
				.ok_or_else(|| tg::error!("checksum build failed"))
		} else {
			Err(tg::error!("could not find the checksum child build"))
		}
	} else {
		Err(tg::error!("could not get checksum build output"))
	}
}
