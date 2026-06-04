use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub specifier: tg::Specifier,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub organization: tg::Organization,
}

impl tg::Session {
	pub async fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/organizations")
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
			return Err(tg::error!(!error, status = %status, "the request failed"));
		}
		response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))
	}
}
