use super::Server;
use tangram_client as tg;
use tangram_error::{error, Result};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::Module,
}

pub type Response = serde_json::Value;

impl Server {
	/// Get the documentation for a module.
	pub async fn doc(&self, module: &tg::Module) -> Result<Response> {
		// Create the request.
		let request = super::Request::Doc(Request {
			module: module.clone(),
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Doc(response) = response else {
			return Err(error!("unexpected response type"));
		};

		Ok(response)
	}
}
