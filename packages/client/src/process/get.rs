use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

pub const METADATA_HEADER: &str = "x-tg-process-metadata";

#[derive(Clone, Debug)]
pub struct Output {
	pub data: tg::process::Data,
	pub metadata: Option<tg::process::Metadata>,
}

impl Output {
	pub fn objects(&self) -> Vec<tg::object::Id> {
		self.data.objects()
	}
}

impl tg::Client {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let method = http::Method::GET;
		let uri = format!("/processes/{id}");
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
			.header_json(tg::process::get::METADATA_HEADER)
			.transpose()?;
		let data = response.json().await?;
		let output = Output { data, metadata };
		Ok(Some(output))
	}
}
