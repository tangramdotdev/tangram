use crate::Server;
use tangram_client as tg;

impl Server {
	pub async fn checksum_artifact(
		&self,
		_artifact: &tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		match algorithm {
			tg::checksum::Algorithm::Unsafe => Ok(tg::Checksum::Unsafe),
			_ => Err(tg::error!("unimplemented")),
		}
	}
}
