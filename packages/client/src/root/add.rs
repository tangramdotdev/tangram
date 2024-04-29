use crate as tg;
use either::Either;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub name: String,
	#[serde(with = "either::serde_untagged")]
	pub id: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn add_root(&self, arg: tg::root::add::Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/roots";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(Outgoing::json(arg))
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
