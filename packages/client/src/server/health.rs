use crate::{self as tg, util::http::empty};
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Health {
	pub version: Option<String>,
}

impl tg::Client {
	pub async fn health(&self) -> tg::Result<Health> {
		let method = http::Method::GET;
		let uri = "/health";
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
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let health = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(health)
	}
}
