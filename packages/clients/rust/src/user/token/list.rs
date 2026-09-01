use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cursor: Option<String>,

	pub data: Vec<tg::user::token::Data>,
}

impl tg::Session {
	pub async fn list_user_tokens(
		&self,
		_arg: tg::user::token::list::Arg,
	) -> tg::Result<tg::user::token::list::Output> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri("/user/tokens")
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
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
			return Err(tg::error!(!error, %status, "the request failed"));
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;

		Ok(output)
	}
}
