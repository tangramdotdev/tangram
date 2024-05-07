use crate as tg;
use either::Either;
use tangram_http::{incoming::ResponseExt as _, outgoing::RequestBuilderExt as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(with = "either::serde_untagged")]
	pub build_or_object: Either<tg::build::Id, tg::object::Id>,
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
