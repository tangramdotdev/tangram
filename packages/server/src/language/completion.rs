use super::{Module, Position, Server};
use lsp_types as lsp;
use tangram_error::{error, Result};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: Module,
	pub position: Position,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub entries: Option<Vec<Entry>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
	pub name: String,
}

impl Server {
	pub(super) async fn handle_completion_request(
		&self,
		params: lsp::CompletionParams,
	) -> Result<Option<lsp::CompletionResponse>> {
		// Get the module.
		let module = self
			.module_for_url(&params.text_document_position.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position.position;

		// Get the completion entries.
		let entries = self.completion(&module, position.into()).await?;
		let Some(entries) = entries else {
			return Ok(None);
		};

		// Convert the completion entries.
		let entries = entries
			.into_iter()
			.map(|completion| lsp::CompletionItem {
				label: completion.name,
				..Default::default()
			})
			.collect();

		Ok(Some(lsp::CompletionResponse::Array(entries)))
	}

	pub async fn completion(
		&self,
		module: &Module,
		position: Position,
	) -> Result<Option<Vec<Entry>>> {
		// Create the request.
		let request = super::Request::Completion(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Completion(response) = response else {
			return Err(error!("Unexpected response type."));
		};

		Ok(response.entries)
	}
}
