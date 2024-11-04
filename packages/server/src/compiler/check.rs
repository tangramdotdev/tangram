use super::Compiler;
use tangram_client as tg;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub modules: Vec<tg::module::Data>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub diagnostics: Vec<tg::Diagnostic>,
}

impl Compiler {
	/// Get all diagnostics for the provided modules.
	pub async fn check(&self, modules: Vec<tg::module::Data>) -> tg::Result<Vec<tg::Diagnostic>> {
		// Create the request.
		let request = super::Request::Check(Request { modules });

		// Perform the request.
		let response = self.request(request).await?.unwrap_check();

		Ok(response.diagnostics)
	}
}
