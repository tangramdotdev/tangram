use crate::{self as tg, util::serde::EitherUntagged};
use either::Either;
use serde_with::{serde_as, FromInto};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub tag: tg::Tag,

	#[serde_as(as = "Option<FromInto<EitherUntagged<tg::build::Id, tg::object::Id>>>")]
	pub item: Option<Either<tg::build::Id, tg::object::Id>>,
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
