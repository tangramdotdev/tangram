use crate::Server;
use tangram_client as tg;

impl Server {
	pub async fn checksum_blob(
		&self,
		blob: &tg::Blob,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		let mut writer = tg::checksum::Writer::new(algorithm);
		let mut reader = blob.reader(self).await?;
		tokio::io::copy(&mut reader, &mut writer)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to copy from the reader to the writer")
			})?;
		let checksum = writer.finalize();
		Ok(checksum)
	}
}
