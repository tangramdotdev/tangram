use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};
use url::Url;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub url: Url,
	pub checksum: tg::Checksum,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub blob: tg::blob::Id,
}

impl tg::Blob {
	pub async fn download<H>(handle: &H, arg: tg::blob::download::Arg) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let output = handle.download_blob(arg).await?;
		let blob = Self::with_id(output.blob);
		Ok(blob)
	}
}

impl tg::Client {
	pub async fn download_blob(
		&self,
		arg: tg::blob::download::Arg,
	) -> tg::Result<tg::blob::download::Output> {
		let method = http::Method::POST;
		let uri = "/blobs/download";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
