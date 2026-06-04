use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub replicate: bool,

	pub tags: Vec<Item>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Item {
	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,
	pub specifier: tg::Specifier,
	pub item: tg::tag::data::Item,
}

impl tg::Session {
	pub async fn post_tag_batch(&self, arg: Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/tags/batch";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(&arg)
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
