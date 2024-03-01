use super::Server;
use lsp_types as lsp;
use tangram_client as tg;
use tangram_error::{error, Result};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::Module,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub locations: Option<Vec<tg::Location>>,
}

impl Server {
	pub async fn definition(
		&self,
		module: &tg::Module,
		position: tg::Position,
	) -> Result<Option<Vec<tg::Location>>> {
		// Create the request.
		let request = super::Request::Definition(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Definition(response) = response else {
			return Err(error!("Unexpected response type."));
		};

		Ok(response.locations)
	}
}

impl Server {
	pub async fn handle_definition_request(
		&self,
		params: lsp::GotoDefinitionParams,
	) -> Result<Option<lsp::GotoDefinitionResponse>> {
		// Get the module.
		let module = self
			.module_for_url(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the definitions.
		let locations = self.definition(&module, position.into()).await?;

		let Some(locations) = locations else {
			return Ok(None);
		};

		// Convert the definitions.
		let locations = locations
			.into_iter()
			.map(|location| lsp::Location {
				uri: self.url_for_module(&location.module),
				range: location.range.into(),
			})
			.collect();

		let response = lsp::GotoDefinitionResponse::Array(locations);

		Ok(Some(response))
	}
}
