use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub format: tg::blob::compress::Format,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub blob: tg::blob::Id,
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
		let blob = Self::with_id(output.blob);
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
