use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{is_default, is_false, is_true, return_true},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub groups: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub organizations: bool,

	pub pattern: tg::specifier::Pattern,

	#[serde(default, skip_serializing_if = "is_false")]
	pub reverse: bool,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub tags: bool,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,

	#[serde(default, skip_serializing_if = "is_default")]
	pub ttl: tg::remote::cache::Ttl,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub users: bool,
}

pub type Entry = tg::list::Entry;
pub type Output = tg::list::Output;

impl Default for Arg {
	fn default() -> Self {
		Self {
			cached: false,
			groups: true,
			length: None,
			location: None,
			organizations: true,
			pattern: tg::specifier::Pattern::default(),
			reverse: false,
			tags: true,
			tokens: tg::authorization::Tokens::default(),
			ttl: tg::remote::cache::Ttl::default(),
			users: true,
		}
	}
}

impl tg::Session {
	pub async fn match_(&self, arg: tg::match_::Arg) -> tg::Result<tg::match_::Output> {
		let method = http::Method::GET;
		let uri = Uri::builder()
			.path("/match")
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
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
