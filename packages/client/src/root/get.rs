use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};
use either::Either;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub name: String,
	#[serde(with = "either::serde_untagged")]
	pub id: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn try_get_root(&self, name: &str) -> tg::Result<Option<tg::root::get::Output>> {
		let method = http::Method::GET;
		let name = urlencoding::encode(name);
		let uri = format!("/roots/{name}");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(Some(output))
	}
}
