use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};
use bytes::Bytes;

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
		let bytes = response.bytes().await?;
		let output = tg::object::get::Output {
			bytes,
			count: None,
			weight: None,
		};
		Ok(Some(output))
	}
}
