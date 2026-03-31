use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub name: String,
	pub url: Uri,
}

impl tg::Client {
	pub async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		let method = http::Method::GET;
		let uri = Uri::builder()
			.path(&format!("/remotes/{name}"))
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.uri(uri.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
