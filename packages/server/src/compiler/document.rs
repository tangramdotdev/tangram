use super::Compiler;
use lsp_types as lsp;
use tangram_client as tg;

/// A document.
#[derive(Clone, Debug)]
pub struct Document {
	pub open: bool,
	pub version: i32,
	pub modified: Option<std::time::SystemTime>,
	pub text: Option<String>,
}

impl Compiler {
	/// List the documents.
	pub async fn list_documents(&self) -> Vec<tg::Module> {
		self.documents
			.iter()
			.filter(|entry| entry.open)
			.map(|entry| entry.key().clone())
			.collect()
	}

	/// Open a document.
	pub async fn open_document(
		&self,
		module: &tg::Module,
		version: i32,
		text: String,
	) -> tg::Result<()> {
		let document = Document {
			open: true,
			version,
			modified: None,
			text: Some(text),
		};
		self.documents.insert(module.clone(), document);
		Ok(())
	}

	/// Update a document.
	pub async fn update_document(
		&self,
		module: &tg::Module,
		range: Option<tg::Range>,
		version: i32,
		text: String,
	) -> tg::Result<()> {
		// Get the document.
		let Some(mut document) = self.documents.get_mut(module) else {
			return Err(tg::error!("could not find the document"));
		};

		// Ensure the document is open.
		if !document.open {
			return Err(tg::error!("expected the document to open"));
		}

		// Update the version.
		document.version = version;

		// Convert the range to bytes.
		let range = if let Some(range) = range {
			range.to_byte_range_in_string(document.text.as_ref().unwrap())
		} else {
			0..document.text.as_mut().unwrap().len()
		};

		// Replace the text.
		document.text.as_mut().unwrap().replace_range(range, &text);

		Ok(())
	}

	/// Close a document.
	pub async fn close_document(&self, module: &tg::Module) -> tg::Result<()> {
		// Get the document.
		let Some(mut document) = self.documents.get_mut(module) else {
			return Err(tg::error!("could not find the document"));
		};

		// Ensure the document is open.
		if !document.open {
			return Err(tg::error!("expected the document to open"));
		}

		// Mark the document as closed.
		document.open = false;

		// Clear the document's text.
		document.text = None;

		// Set the document's modified time.
		let path = match module {
			tg::Module {
				object: tg::module::Object::Path(path),
				..
			} => path.clone(),

			_ => return Err(tg::error!("invalid module")),
		};
		let metadata = tokio::fs::metadata(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
		let modified = metadata.modified().map_err(|error| {
			tg::error!(source = error, "failed to get the last modification time")
		})?;
		document.modified = Some(modified);

		Ok(())
	}
}

impl Compiler {
	pub(super) async fn handle_did_open_notification(
		&self,
		params: lsp::DidOpenTextDocumentParams,
	) -> tg::Result<()> {
		// Get the module.
		let module = self.module_for_uri(&params.text_document.uri).await?;

		// Open the document.
		let version = params.text_document.version;
		let text = params.text_document.text;
		self.open_document(&module, version, text).await?;

		// Update all diagnostics.
		self.update_diagnostics().await?;

		Ok(())
	}

	pub(super) async fn handle_did_change_notification(
		&self,
		params: lsp::DidChangeTextDocumentParams,
	) -> tg::Result<()> {
		// Get the module.
		let module = self.module_for_uri(&params.text_document.uri).await?;

		// Apply the changes.
		for change in params.content_changes {
			self.update_document(
				&module,
				change.range.map(Into::into),
				params.text_document.version,
				change.text,
			)
			.await?;
		}

		// Update all diagnostics.
		self.update_diagnostics().await?;

		Ok(())
	}

	pub(super) async fn handle_did_close_notification(
		&self,
		params: lsp::DidCloseTextDocumentParams,
	) -> tg::Result<()> {
		// Get the module.
		let module = self.module_for_uri(&params.text_document.uri).await?;

		// Close the document.
		self.close_document(&module).await?;

		// Update all diagnostics.
		self.update_diagnostics().await?;

		Ok(())
	}

	pub(super) async fn handle_did_save_notification(
		&self,
		_params: lsp::DidSaveTextDocumentParams,
	) -> tg::Result<()> {
		// Update all diagnostics.
		self.update_diagnostics().await?;

		Ok(())
	}
}
