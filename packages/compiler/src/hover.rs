use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub text: Option<String>,
}

impl Compiler {
	pub(super) async fn handle_hover_request(
		&self,
		params: lsp::HoverParams,
	) -> tg::Result<Option<lsp::Hover>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position_params.text_document.uri)
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

	pub async fn hover(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<String>> {
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
