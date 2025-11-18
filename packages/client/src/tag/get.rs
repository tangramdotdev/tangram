use {
	crate::prelude::*,
	tangram_either::Either,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub tag: tg::Tag,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub item: Option<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		let method = http::Method::GET;
		let uri = format!("/tags/{pattern}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
