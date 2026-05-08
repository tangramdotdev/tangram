use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub namespace: tg::Namespace,
}

impl tg::Session {
	pub async fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<tg::namespace::get::Output>> {
		let method = http::Method::GET;
		let path = format!("/namespaces/{namespace}");
		let uri = Uri::builder().path(&path).build().unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.uri(uri)
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
