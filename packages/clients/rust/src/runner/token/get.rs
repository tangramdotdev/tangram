use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub data: tg::runner::token::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,
}

impl tg::Session {
	pub async fn try_get_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::get::Arg,
	) -> tg::Result<Option<tg::runner::token::get::Output>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/runners/{runner}/tokens/{token}"))
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.arg(&arg, tangram_http::body::Empty::new())
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
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
			return Err(tg::error!(!error, %status, "the request failed"));
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;

		Ok(Some(output))
	}
}
