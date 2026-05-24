use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub namespace: tg::Namespace,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub user: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub group: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub all: bool,

	pub permission: tg::Permission,
}

impl tg::Session {
	pub async fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> tg::Result<tg::Grant> {
		let uri = Uri::builder().path("/namespaces/grants").build().unwrap();
		let request = http::request::Builder::default()
			.method(http::Method::PUT)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
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
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(output)
	}
}
