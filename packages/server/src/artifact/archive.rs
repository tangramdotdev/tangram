use crate::Server;
use tangram_client as tg;

impl Server {
	pub async fn archive_artifact(
		&self,
		_artifact: &tg::Artifact,
		_format: tg::artifact::archive::Format,
	) -> tg::Result<tg::Blob> {
		Err(tg::error!("unimplemented"))
	}
}
