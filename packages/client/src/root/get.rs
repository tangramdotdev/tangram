use crate::{self as tg, util::http::empty};
use either::Either;
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub name: String,
	#[serde(with = "either::serde_untagged")]
	pub id: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn try_get_root(&self, name: &str) -> tg::Result<Option<tg::root::get::Output>> {
		let method = http::Method::GET;
		let name = urlencoding::encode(name);
		let uri = format!("/roots/{name}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
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
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the response body"))?;
		Ok(Some(output))
	}
}
