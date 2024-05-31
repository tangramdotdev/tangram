use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	pub count: Option<u64>,
	pub weight: Option<u64>,
}

pub const HEADER: &str = "x-tg-object-metadata";

impl tg::Client {
	pub async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let method = http::Method::HEAD;
		let uri = format!("/objects/{id}");
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
		let metadata = response
			.header_json::<Metadata>(HEADER)
			.transpose()?
			.ok_or_else(|| tg::error!("expected the metadata header to be set"))?;
		Ok(Some(metadata))
	}
}
