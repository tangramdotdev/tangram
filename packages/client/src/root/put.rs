use crate::{self as tg, util::serde::EitherUntagged};
use either::Either;
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "EitherUntagged<_, _>")]
	pub item: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn put_root(&self, name: &str, arg: tg::root::put::Arg) -> tg::Result<()> {
		let method = http::Method::PUT;
		let uri = format!("/roots/{name}");
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
