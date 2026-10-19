use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
	pub highlights: Option<Vec<Highlight>>,
}

#[derive(Debug)]
pub struct Highlight {
	pub kind: Option<Kind>,
	pub range: tg::Range,
}

#[derive(Debug)]
pub enum Kind {
	Text,
	Read,
	Write,
}

impl Compiler {
	pub async fn document_highlight(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Vec<Highlight>>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			position,
		};

		// Get the output.
		let output = self
			.backend(module.kind)
			.document_highlight(self, arg)
			.await?;

		Ok(output.highlights)
	}
}

impl Compiler {
	pub(crate) async fn handle_document_highlight_request(
		&self,
		params: lsp::DocumentHighlightParams,
	) -> tg::Result<Option<Vec<lsp::DocumentHighlight>>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the highlights.
		let highlights = self.document_highlight(&module, position.into()).await?;
		let Some(highlights) = highlights else {
			return Ok(None);
		};

		// Convert the highlights.
		let highlights = highlights
			.into_iter()
			.map(|highlight| {
				let kind = highlight.kind.map(|kind| match kind {
					Kind::Text => lsp::DocumentHighlightKind::TEXT,
					Kind::Read => lsp::DocumentHighlightKind::READ,
					Kind::Write => lsp::DocumentHighlightKind::WRITE,
				});
				lsp::DocumentHighlight {
					range: highlight.range.into(),
					kind,
				}
			})
			.collect();

		Ok(Some(highlights))
	}
}
