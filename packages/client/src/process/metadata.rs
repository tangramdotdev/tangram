use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_default,
};

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
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
pub struct Metadata {
	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_default")]
	pub children: Children,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_default")]
	pub children_commands: tg::object::Metadata,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "is_default")]
	pub children_outputs: tg::object::Metadata,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
	pub command: tg::object::Metadata,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "is_default")]
	pub output: tg::object::Metadata,
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
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}/metadata?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response
				.json()
				.await
				.map_err(|source| tg::error!(!source, "failed to deserialize the error response"))?;
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
