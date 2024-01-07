use crate::{Location, Module, Position, Server};
use lsp_types as lsp;
use std::collections::HashMap;
use tangram_error::{error, Result};
use url::Url;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: Module,
	pub position: Position,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub locations: Option<Vec<Location>>,
}

impl Server {
	#[allow(clippy::similar_names)]
	pub(super) async fn handle_rename_request(
		&self,
		params: lsp::RenameParams,
	) -> Result<Option<lsp::WorkspaceEdit>> {
		// Get the module.
		let module = self
			.module_for_url(&params.text_document_position.text_document.uri)
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

		// Convert the changes.
		let mut document_changes = HashMap::<Url, lsp::TextDocumentEdit>::new();
		for location in locations {
			// Get the version.
			let version = location
				.module
				.version(Some(&self.inner.document_store))
				.await?;

			// Create the URI.
			let uri = self.url_for_module(&location.module);

			if document_changes.get_mut(&uri).is_none() {
				document_changes.insert(
					uri.clone(),
					lsp::TextDocumentEdit {
						text_document: lsp::OptionalVersionedTextDocumentIdentifier {
							uri: uri.clone(),
							version: Some(version),
						},
						edits: Vec::<lsp::OneOf<lsp::TextEdit, lsp::AnnotatedTextEdit>>::new(),
					},
				);
			}

			document_changes
				.get_mut(&uri)
				.unwrap()
				.edits
				.push(lsp::OneOf::Left(lsp::TextEdit {
					range: location.range.into(),
					new_text: new_text.clone(),
				}));
		}

		let changes = lsp::WorkspaceEdit {
			changes: None,
			document_changes: Some(lsp::DocumentChanges::Edits(
				document_changes.values().cloned().collect(),
			)),
			change_annotations: None,
		};

		Ok(Some(changes))
	}

	pub async fn rename(
		&self,
		module: &Module,
		position: Position,
	) -> Result<Option<Vec<Location>>> {
		// Create the request.
		let request = super::Request::Rename(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Rename(response) = response else {
			return Err(error!("Unexpected response type."));
		};

		Ok(response.locations)
	}
}
