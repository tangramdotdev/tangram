use crate::{self as tg, util::serde::is_false};
use tangram_either::Either;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,

	pub item: Either<tg::process::Id, tg::object::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		let method = http::Method::PUT;
		let uri = format!("/tags/{tag}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
