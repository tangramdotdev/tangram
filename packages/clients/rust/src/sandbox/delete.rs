use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

impl tg::Client {
	pub async fn try_delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<Option<()>> {
		let method = http::Method::DELETE;
		let uri = format!("/sandboxes/{id}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
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
		Ok(Some(()))
	}
}
