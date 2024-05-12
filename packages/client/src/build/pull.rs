use crate as tg;
use tangram_http::{incoming::ResponseExt as _, outgoing::RequestBuilderExt as _};

impl tg::Build {
	pub async fn pull<H1, H2>(&self, _handle: &H1, _remote: &H2) -> tg::Result<()>
	where
		H1: tg::Handle,
		H2: tg::Handle,
	{
		Err(tg::error!("unimplemented"))
	}
}

impl tg::Client {
	pub async fn pull_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/pull");
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
		Ok(())
	}
}
