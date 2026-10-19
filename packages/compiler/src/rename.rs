use {super::Compiler, lsp_types as lsp, std::collections::BTreeMap, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub new_name: String,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
	pub edits: Option<Vec<Data>>,
}

#[derive(Debug)]
pub struct Edit {
	pub location: tg::module::Location,
	pub new_text: String,
}

#[derive(Debug)]
pub struct Data {
	pub location: tg::module::data::Location,
	pub new_text: String,
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

		// Get the edits.
		let edits = self
			.rename(&module, position.into(), params.new_name)
			.await?;

		// Return if there are no edits.
		let Some(edits) = edits else {
			return Ok(None);
		};

		// Convert the edits.
		let mut documents = BTreeMap::<String, lsp::TextDocumentEdit>::new();
		for Edit { location, new_text } in edits {
			// Create the URI.
			let uri = self.lsp_uri_for_module(&location.module.to_data()).await?;

			// Use the editor version for open documents and no version for closed documents.
			let version = self
				.documents
				.get(&super::document::Key::new(&location.module.to_data()))
				.filter(|document| document.open)
				.map(|document| document.version);

			if !documents.contains_key(uri.as_str()) {
				let entry = lsp::TextDocumentEdit {
					text_document: lsp::OptionalVersionedTextDocumentIdentifier {
						uri: uri.clone(),
						version,
					},
					edits: Vec::<lsp::OneOf<lsp::TextEdit, lsp::AnnotatedTextEdit>>::new(),
				};
				documents.insert(uri.to_string(), entry);
			}

			documents
				.get_mut(uri.as_str())
				.unwrap()
				.edits
				.push(lsp::OneOf::Left(lsp::TextEdit {
					range: location.range.into(),
					new_text,
				}));
		}

		let edit = lsp::WorkspaceEdit {
			changes: None,
			document_changes: Some(lsp::DocumentChanges::Edits(
				documents.into_values().collect(),
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
	) -> tg::Result<Option<Vec<Edit>>> {
		if self.is_generated_module(module) {
			return Ok(None);
		}

		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			new_name,
			position,
		};

		// Get the output.
		let output = self.backend(module.kind).rename(self, arg).await?;

		// A rename must never apply generated positions to a source in another language.
		if output.edits.as_ref().is_some_and(|edits| {
			edits.iter().any(|edit| {
				let location = &edit.location;
				(location.module.kind == tg::module::Kind::Python)
					!= (module.kind == tg::module::Kind::Python)
					|| self.is_generated_module(&location.module)
			})
		}) {
			return Ok(None);
		}

		// Convert the edits from data to the non-serializable form.
		let edits = output
			.edits
			.map(|edits| {
				edits
					.into_iter()
					.map(|edit| {
						let location = edit.location.try_into()?;
						let new_text = edit.new_text;
						let edit = Edit { location, new_text };
						Ok::<_, tg::Error>(edit)
					})
					.collect::<tg::Result<_>>()
			})
			.transpose()?;

		Ok(edits)
	}
}
