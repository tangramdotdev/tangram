use crate::{self as tg, util::http::empty};
use http_body_util::BodyExt as _;

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
		let body = empty();
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
		Ok(())
	}
}
