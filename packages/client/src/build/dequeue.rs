use crate::{self as tg, util::http::full};
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub timeout: Option<std::time::Duration>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::build::Id,
}

impl tg::Client {
	pub async fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
		_stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<tg::build::dequeue::Output>> {
		let method = http::Method::POST;
		let uri = "/builds/dequeue";
		let mut request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let json = serde_json::to_vec(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(json);
		let request = request
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
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(Some(output))
	}
}
