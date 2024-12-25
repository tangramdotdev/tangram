use super::Compiler;
use lsp_types as lsp;
use std::collections::HashMap;
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
	pub locations: Option<Vec<tg::Location>>,
}

impl Compiler {
	pub(super) async fn handle_rename_request(
		&self,
		params: lsp::RenameParams,
	) -> tg::Result<Option<lsp::WorkspaceEdit>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position.position;
		let new_text = &params.new_name;

		// Get the references.
		let locations = self.rename(&module, position.into()).await?;

		// If there are no references, then return None.
		let Some(locations) = locations else {
			return Ok(None);
		};

		// Convert the edits.
		#[allow(clippy::mutable_key_type)]
		let mut edit = HashMap::<lsp::Uri, lsp::TextDocumentEdit>::new();
		for location in locations {
			// Create the URI.
			let uri = self.lsp_uri_for_module(&location.module).await?;

			// Get the version.
			let version = self.get_module_version(&location.module).await?;

			if edit.get_mut(&uri).is_none() {
				edit.insert(uri.clone(), lsp::TextDocumentEdit {
					text_document: lsp::OptionalVersionedTextDocumentIdentifier {
						uri: uri.clone(),
						version: Some(version),
					},
					edits: Vec::<lsp::OneOf<lsp::TextEdit, lsp::AnnotatedTextEdit>>::new(),
				});
			}

			edit.get_mut(&uri)
				.unwrap()
				.edits
				.push(lsp::OneOf::Left(lsp::TextEdit {
					range: location.range.into(),
					new_text: new_text.clone(),
				}));
		}

		let edit = lsp::WorkspaceEdit {
			changes: None,
			document_changes: Some(lsp::DocumentChanges::Edits(
				edit.values().cloned().collect(),
			)),
			change_annotations: None,
		};

		Ok(Some(edit))
	}

	pub async fn rename(
		&self,
		module: &tg::Module,
		position: tg::Position,
	) -> tg::Result<Option<Vec<tg::Location>>> {
		// Create the request.
		let request = super::Request::Rename(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Rename(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.locations)
	}
}
