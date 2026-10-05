use {super::Compiler, lsp_types as lsp, std::collections::HashMap, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::module::Data,
	pub new_name: String,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub locations: Option<Vec<tg::module::data::Location>>,
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
		let locations = self
			.rename(&module, position.into(), params.new_name.clone())
			.await?;

		// If there are no references, then return None.
		let Some(locations) = locations else {
			return Ok(None);
		};

		// Convert the edits.
		#[expect(clippy::mutable_key_type)]
		let mut edit = HashMap::<lsp::Uri, lsp::TextDocumentEdit>::new();
		for location in locations {
			// Create the URI.
			let uri = self.lsp_uri_for_module(&location.module.to_data()).await?;

			// Use the editor version for open documents and no version for closed documents.
			let version = self
				.documents
				.get(&super::document::Key::new(&location.module.to_data()))
				.filter(|document| document.open)
				.map(|document| document.version);

			if edit.get_mut(&uri).is_none() {
				let entry = lsp::TextDocumentEdit {
					text_document: lsp::OptionalVersionedTextDocumentIdentifier {
						uri: uri.clone(),
						version,
					},
					edits: Vec::<lsp::OneOf<lsp::TextEdit, lsp::AnnotatedTextEdit>>::new(),
				};
				edit.insert(uri.clone(), entry);
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
		module: &tg::module::Data,
		position: tg::Position,
		new_name: String,
	) -> tg::Result<Option<Vec<tg::module::Location>>> {
		if self.is_generated_module(module) {
			return Ok(None);
		}

		// Create the request.
		let request = super::Request::Rename(Request {
			module: module.clone(),
			new_name,
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Rename(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		// A rename must never apply generated positions to a source in another language.
		if response.locations.as_ref().is_some_and(|locations| {
			locations.iter().any(|location| {
				(location.module.kind == tg::module::Kind::Py)
					!= (module.kind == tg::module::Kind::Py)
					|| self.is_generated_module(&location.module)
			})
		}) {
			return Ok(None);
		}

		// Convert locations from data to the non-serializable form.
		let locations = response
			.locations
			.map(|locations| {
				locations
					.into_iter()
					.map(TryInto::try_into)
					.collect::<tg::Result<Vec<_>>>()
			})
			.transpose()?;

		Ok(locations)
	}

	fn is_generated_module(&self, module: &tg::module::Data) -> bool {
		matches!(&module.referent.node, tg::module::data::Source::Path(path) if path.starts_with(self.library_path.join("generated")))
	}
}
