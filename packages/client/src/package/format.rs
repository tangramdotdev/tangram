use crate::{self as tg, util::http::empty};
use http_body_util::BodyExt as _;

impl tg::Client {
	pub async fn format_package(&self, dependency: &tg::Dependency) -> tg::Result<()> {
		let method = http::Method::POST;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/format");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = empty();
		let request = request
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
				.unwrap_or_else(|_| tg::error!("failed to deserialize the error"));
			return Err(error);
		}
		Ok(())
	}
}
