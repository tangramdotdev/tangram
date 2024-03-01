use super::{Module, Position, Result, Server};
use lsp_types as lsp;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: Module,
	pub position: Position,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub text: Option<String>,
}

impl Server {
	pub(super) async fn handle_hover_request(
		&self,
		params: lsp::HoverParams,
	) -> Result<Option<lsp::Hover>> {
		// Get the module.
		let module = self
			.module_for_url(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the hover info.
		let hover = self.hover(&module, position.into()).await?;
		let Some(hover) = hover else {
			return Ok(None);
		};

		// Create the hover.
		let hover = lsp::Hover {
			contents: lsp::HoverContents::Scalar(lsp::MarkedString::from_language_code(
				"typescript".into(),
				hover,
			)),
			range: None,
		};

		Ok(Some(hover))
	}

	pub async fn hover(&self, module: &Module, position: Position) -> Result<Option<String>> {
		// Create the request.
		let request = super::Request::Hover(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?.unwrap_hover();

		Ok(response.text)
	}
}
