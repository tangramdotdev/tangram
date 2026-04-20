use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub replicate: bool,

	pub tags: Vec<Item>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Item {
	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,
	pub tag: tg::Tag,
	pub item: tg::Either<tg::object::Id, tg::process::Id>,
}

impl tg::Client {
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
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		Ok(())
	}
}
