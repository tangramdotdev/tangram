use super::Compiler;
use tangram_client as tg;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::Module,
}

pub type Response = serde_json::Value;

impl Compiler {
	/// Get the documentation for a module.
	pub async fn doc(&self, module: &tg::Module) -> tg::Result<Response> {
		// Create the request.
		let request = super::Request::Doc(Request {
			module: module.clone(),
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Doc(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response)
	}
}
