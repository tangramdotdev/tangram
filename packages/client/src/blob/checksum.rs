use crate as tg;
use tangram_http::{incoming::ResponseExt as _, outgoing::RequestBuilderExt as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub algorithm: tg::checksum::Algorithm,
}

pub type Output = tg::Checksum;

impl tg::Blob {
	pub async fn checksum<H>(
		&self,
		handle: &H,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let arg = Arg { algorithm };
		let checksum = handle.checksum_blob(&id, arg).await?;
		Ok(checksum)
	}
}

impl tg::Client {
	pub async fn checksum_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::checksum::Arg,
	) -> tg::Result<tg::blob::checksum::Output> {
		let method = http::Method::POST;
		let uri = format!("/blobs/{id}/checksum");
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
