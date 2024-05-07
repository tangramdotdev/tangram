use crate as tg;
use tangram_http::{incoming::ResponseExt as _, outgoing::RequestBuilderExt as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub blob: tg::blob::Id,
	pub format: Option<tg::artifact::archive::Format>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub artifact: tg::artifact::Id,
}

impl tg::Artifact {
	pub async fn extract<H>(
		handle: &H,
		blob: &tg::Blob,
		format: Option<tg::artifact::archive::Format>,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let blob = blob.id(handle, None).await?;
		let arg = Arg { blob, format };
		let output = handle.extract_artifact(arg).await?;
		let artifact = Self::with_id(output.artifact);
		Ok(artifact)
	}
}

impl tg::Client {
	pub async fn extract_artifact(
		&self,
		arg: tg::artifact::extract::Arg,
	) -> tg::Result<tg::artifact::extract::Output> {
		let method = http::Method::POST;
		let uri = "/artifacts/extract";
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
