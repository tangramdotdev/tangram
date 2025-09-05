use crate::{self as tg, util::serde::is_default};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Metadata {
	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_default")]
	pub children: Children,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
	pub commands: tg::object::Metadata,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_default")]
	pub outputs: tg::object::Metadata,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Children {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,
}

impl tg::Client {
	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
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
