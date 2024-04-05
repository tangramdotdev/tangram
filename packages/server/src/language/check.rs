use super::Server;
use tangram_client as tg;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub modules: Vec<tg::Module>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub diagnostics: Vec<tg::Diagnostic>,
}

impl Server {
	/// Get all diagnostics for the provided modules.
	pub async fn check(&self, modules: Vec<tg::Module>) -> tg::Result<Vec<tg::Diagnostic>> {
		// Create the request.
		let request = super::Request::Check(Request { modules });

		// Perform the request.
		let response = self.request(request).await?.unwrap_check();

		Ok(response.diagnostics)
	}
}
