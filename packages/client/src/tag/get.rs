use crate as tg;
use tangram_either::Either;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub tag: tg::Tag,

	pub item: Either<tg::process::Id, tg::object::Id>,

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
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(Some(output))
	}
}
