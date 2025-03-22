use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(flatten)]
	pub metadata: Metadata,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_weight: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_weight: Option<u64>,
}

impl tg::Client {
	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::metadata::Output>> {
		let method = http::Method::GET;
		let uri = format!("/processes/{id}/metadata");
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
