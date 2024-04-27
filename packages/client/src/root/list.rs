use crate::{self as tg, util::http::empty};
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub items: Vec<tg::root::get::Output>,
}

impl tg::Client {
	pub async fn list_roots(&self, arg: tg::root::list::Arg) -> tg::Result<tg::root::list::Output> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the search params"))?;
		let uri = format!("/roots?{search_params}");
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
		Ok(output)
	}
}
