use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub range: tg::Range,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub hints: Option<Vec<Hint>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hint {
	pub position: tg::Position,
	pub label: String,
	pub kind: Option<Kind>,
	pub padding_left: Option<bool>,
	pub padding_right: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
		// Create the request.
		let request = super::Request::InlayHint(Request {
			module: module.clone(),
			range,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::InlayHint(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.hints)
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
