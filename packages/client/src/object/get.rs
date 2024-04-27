use crate::{self as tg, util::http::empty};
use bytes::Bytes;
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub bytes: Bytes,
	pub count: Option<u64>,
	pub weight: Option<u64>,
}

impl tg::Client {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let method = http::Method::GET;
		let uri = format!("/objects/{id}");
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
		let output = tg::object::get::Output {
			bytes,
			count: None,
			weight: None,
		};
		Ok(Some(output))
	}
}
