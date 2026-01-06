use {
	crate::prelude::*,
	bytes::Bytes,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_false},
};

pub const METADATA_HEADER: &str = "x-tg-object-metadata";

#[serde_as]
#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Bytes,
	pub metadata: Option<tg::object::Metadata>,
}

impl tg::Client {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/objects/{id}?{query}");
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
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let metadata = response
			.header_json(METADATA_HEADER)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize the metadata header"))?;
		let bytes = response
			.bytes()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the response body"))?;
		let output = tg::object::get::Output { bytes, metadata };
		Ok(Some(output))
	}
}
