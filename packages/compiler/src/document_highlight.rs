use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub highlights: Option<Vec<Highlight>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Highlight {
	pub range: tg::Range,
	pub kind: Option<Kind>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
		// Create the request.
		let request = super::Request::DocumentHighlight(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::DocumentHighlight(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.highlights)
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
