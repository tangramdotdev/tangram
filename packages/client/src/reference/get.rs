use crate as tg;
use either::Either;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	#[serde(with = "either::serde_untagged")]
	pub item: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::reference::get::Output>> {
		let method = http::Method::GET;
		let path = reference.uri().path();
		let query = reference.uri().query().unwrap_or_default();
		let uri = format!("/references/{path}?{query}");
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