use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_false},
};

#[serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,

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
		let response = self
			.send(|| {
				http::request::Builder::default()
					.method(http::Method::POST)
					.uri("/tags/batch")
					.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
					.header(
						http::header::CONTENT_TYPE,
						mime::APPLICATION_JSON.to_string(),
					)
					.json(arg.clone())
					.unwrap()
					.unwrap()
			})
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
