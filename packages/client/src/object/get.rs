use {
	crate as tg,
	bytes::Bytes,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Bytes,
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
		let output = tg::object::get::Output { bytes };
		Ok(Some(output))
	}
}
