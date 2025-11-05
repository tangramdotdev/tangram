use {
	crate::prelude::*,
	tangram_either::Either,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	pub tags: Vec<Item>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Item {
	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,
	pub tag: tg::Tag,
	pub item: Either<tg::process::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn post_tag_batch(&self, arg: Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/tags/batch";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
