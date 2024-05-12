use crate as tg;
use bytes::Bytes;
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub bytes: Bytes,
	pub count: Option<u64>,
	pub weight: Option<u64>,
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
		let bytes = response.bytes().await?;
		let output = tg::object::get::Output {
			bytes,
			count: None,
			weight: None,
		};
		Ok(Some(output))
	}
}
