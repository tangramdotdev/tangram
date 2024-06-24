use crate::{self as tg, util::serde::EitherUntagged};
use either::Either;
use serde_with::{serde_as, FromInto};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub force: bool,

	#[serde_as(as = "FromInto<EitherUntagged<tg::build::Id, tg::object::Id>>")]
	pub item: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		let method = http::Method::PUT;
		let uri = format!("/tags/{tag}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
