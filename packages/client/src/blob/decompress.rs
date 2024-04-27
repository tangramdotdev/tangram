use crate::{self as tg, util::http::full};
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub format: tg::blob::compress::Format,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::blob::Id,
}

impl tg::Blob {
	pub async fn decompress<H>(
		&self,
		handle: &H,
		format: tg::blob::compress::Format,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let arg = Arg { format };
		let output = handle.decompress_blob(&id, arg).await?;
		let blob = Self::with_id(output.id);
		Ok(blob)
	}
}

impl tg::Client {
	pub async fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::decompress::Arg,
	) -> tg::Result<tg::blob::decompress::Output> {
		let method = http::Method::POST;
		let uri = format!("/blobs/{id}/decompress");
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}
}
