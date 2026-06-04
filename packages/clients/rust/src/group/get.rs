use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

impl tg::Session {
	pub async fn try_get_group(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<Option<tg::Group>> {
		let uri = format!("/groups/{}", group.to_string().replace('/', ":"));
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
