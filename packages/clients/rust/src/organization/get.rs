use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{is_default, is_false},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub ttl: tg::remote::cache::Ttl,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::organization::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	pub name: String,
	pub specifier: tg::Specifier,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

impl tg::Session {
	pub async fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::get::Arg,
	) -> tg::Result<Option<tg::organization::get::Output>> {
		let path = format!(
			"/organizations/{}",
			organization.to_string().replace('/', ":")
		);
		let path = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(path)
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
			return Err(tg::error!(!error, status = %status, "the request failed"));
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
