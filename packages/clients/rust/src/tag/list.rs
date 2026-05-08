use {
	crate::prelude::*,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

#[serde_as]
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "tg::tag::Pattern::is_empty")]
	pub pattern: tg::tag::Pattern,

	#[serde(default, skip_serializing_if = "is_false")]
	pub recursive: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub reverse: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub data: Vec<Entry>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct Entry {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub item: Option<tg::Either<tg::object::Id, tg::process::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	pub tag: tg::Tag,
}

impl tg::Session {
	pub async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		let method = http::Method::GET;
		let uri = Uri::builder()
			.path("/tags")
			.query_params(&arg)
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
