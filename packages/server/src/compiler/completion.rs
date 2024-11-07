use super::Compiler;
use lsp_types as lsp;
use tangram_client as tg;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::Module,
	pub position: tg::Position,
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

impl Compiler {
	pub async fn completion(
		&self,
		module: &tg::Module,
		position: tg::Position,
	) -> tg::Result<Option<Vec<Entry>>> {
		// Create the request.
		let request = super::Request::Completion(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Completion(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.entries)
	}
}

impl Compiler {
	pub(super) async fn handle_completion_request(
		&self,
		params: lsp::CompletionParams,
	) -> tg::Result<Option<lsp::CompletionResponse>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position.text_document.uri)
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
}
