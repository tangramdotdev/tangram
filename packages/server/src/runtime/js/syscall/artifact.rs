use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn archive(
	state: Rc<State>,
	args: (tg::Artifact, tg::artifact::archive::Format),
) -> tg::Result<tg::Blob> {
	let server = &state.server;
	let (artifact, format) = args;
	let blob = server.archive_artifact(&artifact, format).await?;
	Ok(blob)
}

pub async fn checksum(
	state: Rc<State>,
	args: (tg::Artifact, tg::checksum::Algorithm),
) -> tg::Result<tg::Checksum> {
	let server = &state.server;
	let (artifact, algorithm) = args;
	let checksum = server.checksum_artifact(&artifact, algorithm).await?;
	Ok(checksum)
}

pub async fn bundle(state: Rc<State>, args: (tg::Artifact,)) -> tg::Result<tg::Artifact> {
	let server = &state.server;
	let (artifact,) = args;
	let artifact = server.bundle_artifact(&artifact).await?;
	Ok(artifact)
}

pub async fn extract(
	state: Rc<State>,
	args: (tg::Blob, tg::artifact::archive::Format),
) -> tg::Result<tg::Artifact> {
	let server = &state.server;
	let (blob, format) = args;
	let format = Some(format);
	let blob = server.extract_artifact(&blob, format).await?;
	Ok(blob)
}
