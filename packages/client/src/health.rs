use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub fields: Option<Vec<String>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Health {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub diagnostics: Option<Vec<tg::diagnostic::Data>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pipes: Option<Vec<tg::pipe::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub processes: Option<Processes>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ptys: Option<Vec<tg::pty::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Processes {
	pub created: u64,

	pub dequeued: u64,

	pub enqueued: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permits: Option<u64>,

	pub started: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Database {
	pub available_connections: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FileDescriptorSemaphore {
	pub available_permits: u64,
}

impl tg::Client {
	pub async fn health(&self, arg: tg::health::Arg) -> tg::Result<Health> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/health?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(output)
	}
}
