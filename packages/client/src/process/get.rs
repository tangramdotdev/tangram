use {
	crate::prelude::*,
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_false},
};

pub const METADATA_HEADER: &str = "x-tg-process-metadata";

#[serde_as]
#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::process::Id,

	#[serde(flatten)]
	pub data: tg::process::Data,

	#[serde(skip)]
	pub metadata: Option<tg::process::Metadata>,
}

impl tg::Client {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}?{query}");
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
		let mut output = response
			.json::<tg::process::get::Output>()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		if let Some(metadata) = metadata {
			output.metadata = Some(metadata);
		}
		Ok(Some(output))
	}
}
