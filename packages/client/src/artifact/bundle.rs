use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub artifact: tg::artifact::Id,
}

impl tg::Artifact {
	pub async fn bundle<H>(&self, handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let output = handle.bundle_artifact(&id).await?;
		let artifact = Self::with_id(output.artifact);
		Ok(artifact)
	}
}

impl tg::Client {
	pub async fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> tg::Result<tg::artifact::bundle::Output> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/bundle");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
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
