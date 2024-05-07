use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn archive(
	state: Rc<State>,
	args: (tg::Artifact, tg::artifact::archive::Format),
) -> tg::Result<tg::Blob> {
	let server = &state.server;
	let (artifact, format) = args;
	let id = artifact.id(server, None).await?;
	let arg = tg::artifact::archive::Arg { format };
	let output = server.archive_artifact(&id, arg).await?;
	let blob = tg::Blob::with_id(output.blob);
	Ok(blob)
}

pub async fn checksum(
	state: Rc<State>,
	args: (tg::Artifact, tg::checksum::Algorithm),
) -> tg::Result<tg::Checksum> {
	let server = &state.server;
	let (artifact, algorithm) = args;
	let artifact = artifact.id(server, None).await?;
	let arg = tg::artifact::checksum::Arg { algorithm };
	let checksum = server.checksum_artifact(&artifact, arg).await?;
	Ok(checksum)
}

pub async fn bundle(state: Rc<State>, args: (tg::Artifact,)) -> tg::Result<tg::Artifact> {
	let server = &state.server;
	let (artifact,) = args;
	let id = artifact.id(server, None).await?;
	let output = server.bundle_artifact(&id).await?;
	let artifact = tg::Artifact::with_id(output.artifact);
	Ok(artifact)
}

pub async fn extract(
	state: Rc<State>,
	args: (tg::Blob, tg::artifact::archive::Format),
) -> tg::Result<tg::Artifact> {
	let server = &state.server;
	let (blob, format) = args;
	let blob = blob.id(server, None).await?;
	let format = Some(format);
	let arg = tg::artifact::extract::Arg { blob, format };
	let output = server.extract_artifact(arg).await?;
	let archive = tg::Artifact::with_id(output.artifact);
	Ok(archive)
}
