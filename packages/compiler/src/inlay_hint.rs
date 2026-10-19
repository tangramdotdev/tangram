use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub range: tg::Range,
}

#[derive(Debug)]
pub struct Output {
	pub hints: Option<Vec<Hint>>,
}

#[derive(Debug)]
pub struct Hint {
	pub position: tg::Position,
	pub label: String,
	pub kind: Option<Kind>,
	pub padding_left: Option<bool>,
	pub padding_right: Option<bool>,
}

#[derive(Debug)]
pub enum Kind {
	Type,
	Parameter,
}

impl Compiler {
	pub async fn inlay_hints(
		&self,
		module: &tg::module::Data,
		range: tg::Range,
	) -> tg::Result<Option<Vec<Hint>>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			range,
		};

		// Get the output.
		let output = self.backend(module.kind).inlay_hint(self, arg).await?;

		Ok(output.hints)
	}
}

impl Compiler {
	pub(crate) async fn handle_inlay_hint_request(
		&self,
		params: lsp::InlayHintParams,
	) -> tg::Result<Option<Vec<lsp::InlayHint>>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the range for the request.
		let range = params.range;

		// Get the inlay hints.
		let hints = self.inlay_hints(&module, range.into()).await?;
		let Some(hints) = hints else {
			return Ok(None);
		};

		// Convert the inlay hints.
		let hints = hints
			.into_iter()
			.map(|hint| {
				let kind = hint.kind.map(|kind| match kind {
					Kind::Type => lsp::InlayHintKind::TYPE,
					Kind::Parameter => lsp::InlayHintKind::PARAMETER,
				});
				lsp::InlayHint {
					position: hint.position.into(),
					label: lsp::InlayHintLabel::String(hint.label),
					kind,
					text_edits: None,
					tooltip: None,
					padding_left: hint.padding_left,
					padding_right: hint.padding_right,
					data: None,
				}
			})
			.collect();

		Ok(Some(hints))
	}
}
