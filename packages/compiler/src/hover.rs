use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
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
			contents: if module.kind == tg::module::Kind::Python {
				lsp::HoverContents::Markup(lsp::MarkupContent {
					kind: lsp::MarkupKind::Markdown,
					value: hover,
				})
			} else {
				lsp::HoverContents::Scalar(lsp::MarkedString::from_language_code(
					"typescript".into(),
					hover,
				))
			},
			range: None,
		};

		Ok(Some(hover))
	}

	pub async fn hover(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<String>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			position,
		};

		// Get the output.
		let output = self.backend(module.kind).hover(self, arg).await?;

		Ok(output.text)
	}
}
