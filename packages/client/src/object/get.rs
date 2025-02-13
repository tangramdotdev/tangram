use crate as tg;
use bytes::Bytes;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

pub const METADATA_HEADER: &str = "x-tg-object-metadata";

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Bytes,
	pub metadata: Option<tg::object::Metadata>,
}

impl tg::Client {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let method = http::Method::GET;
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
			.header_json(tg::object::get::METADATA_HEADER)
			.transpose()?;
		let bytes = response.bytes().await?;
		let output = tg::object::get::Output { bytes, metadata };
		Ok(Some(output))
	}
}
