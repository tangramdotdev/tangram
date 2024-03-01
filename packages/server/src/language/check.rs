use super::{Diagnostic, Module, Server};
use tangram_error::Result;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub modules: Vec<Module>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub diagnostics: Vec<Diagnostic>,
}

impl Server {
	/// Get all diagnostics for the provided modules.
	pub async fn check(&self, modules: Vec<Module>) -> Result<Vec<Diagnostic>> {
		// Create the request.
		let request = super::Request::Check(Request { modules });

		// Perform the request.
		let response = self.request(request).await?.unwrap_check();

		Ok(response.diagnostics)
	}
}
