use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub namespace: tg::Namespace,
}

impl tg::Session {
	pub async fn create_namespace(&self, namespace: &tg::Namespace) -> tg::Result<()> {
		let method = http::Method::PUT;
		let uri = Uri::builder().path("/namespaces").build().unwrap();
		let arg = tg::namespace::create::Arg {
			namespace: namespace.clone(),
		};
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		Ok(())
	}
}
