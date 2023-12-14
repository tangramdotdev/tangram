use crate::{return_error, Module, Result, Server};
use std::collections::BTreeMap;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: Module,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub exports: BTreeMap<String, serde_json::Value>,
}

impl Server {
	/// Get the docs for a module.
	pub async fn docs(&self, module: &Module) -> Result<Response> {
		// Create the request.
		let request = super::Request::Docs(Request {
			module: module.clone(),
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Docs(response) = response else {
			return_error!("Unexpected response type.")
		};

		Ok(response)
	}
}
