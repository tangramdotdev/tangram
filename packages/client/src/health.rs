use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Health {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub diagnostics: Vec<tg::diagnostic::Data>,

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
	pub async fn health(&self) -> tg::Result<Health> {
		let method = http::Method::GET;
		let uri = "/health";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send(request)
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
