use crate::Server;
use tangram_client as tg;
use tangram_http::{
	incoming::RequestExt as _, outgoing::ResponseBuilderExt as _, Incoming, Outgoing,
};

impl Server {
	pub async fn checksum_blob(
		&self,
		blob: &tg::blob::Id,
		arg: tg::blob::checksum::Arg,
	) -> tg::Result<tg::Checksum> {
		let blob = tg::Blob::with_id(blob.clone());
		let mut writer = tg::checksum::Writer::new(arg.algorithm);
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

impl Server {
	pub(crate) async fn handle_checksum_blob_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.checksum_blob(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
