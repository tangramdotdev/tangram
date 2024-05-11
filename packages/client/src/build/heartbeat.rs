use crate as tg;
use tangram_http::{incoming::ResponseExt as _, outgoing::RequestBuilderExt};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub stop: bool,
}

impl tg::Build {
	pub async fn heartbeat<H>(&self, handle: &H) -> tg::Result<tg::build::heartbeat::Output>
	where
		H: tg::Handle,
	{
		handle.heartbeat_build(self.id()).await
	}
}

impl tg::Client {
	pub async fn heartbeat_build(&self, id: &tg::build::Id) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/heartbeat");
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
