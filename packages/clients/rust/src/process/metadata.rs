use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_default,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<tg::grant::Token>,
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
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub node: Node,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub subtree: Subtree,
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
pub struct Node {
	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub command: tg::object::metadata::Subtree,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub error: tg::object::metadata::Subtree,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub log: tg::object::metadata::Subtree,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_default")]
	pub output: tg::object::metadata::Subtree,
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
pub struct Subtree {
	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub command: tg::object::metadata::Subtree,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub count: Option<u64>,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "is_default")]
	pub depth: Option<u64>,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub error: tg::object::metadata::Subtree,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_default")]
	pub log: tg::object::metadata::Subtree,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_default")]
	pub output: tg::object::metadata::Subtree,
}

impl Metadata {
	pub fn merge(&mut self, other: &Self) {
		self.node.merge(&other.node);
		self.subtree.merge(&other.subtree);
	}
}

impl Node {
	pub fn merge(&mut self, other: &Self) {
		self.command.merge(&other.command);
		self.error.merge(&other.error);
		self.log.merge(&other.log);
		self.output.merge(&other.output);
	}
}

impl Subtree {
	pub fn merge(&mut self, other: &Self) {
		if self.count.is_none() {
			self.count = other.count;
		}
		if self.depth.is_none() {
			self.depth = other.depth;
		}
		self.command.merge(&other.command);
		self.error.merge(&other.error);
		self.log.merge(&other.log);
		self.output.merge(&other.output);
	}
}

impl tg::Session {
	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let method = http::Method::GET;
		let path = format!("/processes/{id}/metadata");
		let uri = Uri::builder()
			.path(&path)
			.query_params(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
