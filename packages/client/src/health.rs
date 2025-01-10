use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Health {
	pub builds: Option<Builds>,
	pub database: Option<Database>,
	pub file_descriptor_semaphore: Option<FileDescriptorSemaphore>,
	pub version: Option<String>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub diagnostics: Vec<tg::Diagnostic>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Builds {
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

/// Return the compiled version string.
#[must_use]
pub fn version() -> String {
	let mut version = env!("CARGO_PKG_VERSION").to_owned();
	if let Some(commit) = option_env!("TANGRAM_CLI_COMMIT_HASH") {
		version.push('+');
		version.push_str(commit);
	}
	version
}
