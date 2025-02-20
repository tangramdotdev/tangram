use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(
	Clone, Debug, Default, PartialEq, PartialOrd, Eq, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct Metadata {
	pub complete: bool,
	pub count: Option<u64>,
	pub depth: Option<u64>,
	pub weight: Option<u64>,
}

impl tg::Client {
	pub async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let method = http::Method::GET;
		let uri = format!("/objects/{id}/metadata");
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
		let metadata = response.json().await?;
		Ok(Some(metadata))
	}
}
