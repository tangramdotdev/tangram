use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Health {
	pub processes: Option<Processes>,
	pub database: Option<Database>,
	pub file_descriptor_semaphore: Option<FileDescriptorSemaphore>,
	pub version: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Processes {
	pub created: u64,
	pub dequeued: u64,
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
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
