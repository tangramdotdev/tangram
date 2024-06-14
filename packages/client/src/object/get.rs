use crate::{self as tg, util::serde::BytesBase64};
use bytes::Bytes;
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,
	pub metadata: tg::object::Metadata,
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
			.header_json::<tg::object::Metadata>(tg::object::metadata::HEADER)
			.transpose()?
			.ok_or_else(|| tg::error!("expected the metadata header to be set"))?;
		let bytes = response.bytes().await?;
		let output = tg::object::get::Output { bytes, metadata };
		Ok(Some(output))
	}
}
