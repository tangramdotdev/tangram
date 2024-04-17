use crate::{
	util::http::{bad_request, full, Incoming, Outgoing},
	Http, Server,
};
use http_body_util::BodyExt as _;
use std::pin::Pin;
use tangram_client as tg;
use tokio::io::AsyncRead;

impl Server {
	pub async fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::CompressArg,
	) -> tg::Result<tg::blob::CompressOutput> {
		let blob = tg::Blob::with_id(id.clone());
		let reader = blob.reader(self).await?;
		let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match arg.format {
			tg::blob::CompressionFormat::Bz2 => {
				Box::pin(async_compression::tokio::bufread::BzEncoder::new(reader))
			},
			tg::blob::CompressionFormat::Gz => {
				Box::pin(async_compression::tokio::bufread::GzipEncoder::new(reader))
			},
			tg::blob::CompressionFormat::Xz => {
				Box::pin(async_compression::tokio::bufread::XzEncoder::new(reader))
			},
			tg::blob::CompressionFormat::Zstd => {
				Box::pin(async_compression::tokio::bufread::ZstdEncoder::new(reader))
			},
		};
		let blob = tg::Blob::with_reader(self, reader, None).await?;
		let id = blob.id(self, None).await?;
		let output = tg::blob::CompressOutput { id };
		Ok(output)
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_compress_blob_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["blobs", id, "compress"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Compress the blob.
		let output = self.inner.tg.compress_blob(&id, arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the response"))?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
