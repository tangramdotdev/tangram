use std::path::PathBuf;

use super::Compiler;
use lsp_types as lsp;
use tangram_client as tg;

impl Compiler {
	/// Get all the server's documents.
	pub async fn get_documents(&self) -> Vec<tg::Module> {
		let documents = self.documents.read().await;
		documents.keys().cloned().collect()
	}

	/// Get a document.
	pub async fn get_document(
		&self,
		package_path: PathBuf,
		module_path: tg::Path,
	) -> tg::Result<tg::Module> {
		// Get the module.
		let module =
			tg::Module::with_package_path(&self.server, package_path.clone(), module_path.clone())
				.await?;

		// Add to the documents if necessary.
		let mut documents = self.documents.write().await;
		if !documents.contains_key(&module) {
			let metadata = tokio::fs::metadata(&package_path.join(&module_path))
				.await
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
			let modified = metadata.modified().map_err(|error| {
				tg::error!(source = error, "failed to get the last modification time")
			})?;
			let state = tg::document::State::Closed(tg::document::Closed {
				version: 0,
				modified,
			});
			documents.insert(module.clone(), state);
		}

		Ok(module)
	}

	/// Open a document.
	pub async fn open_document(
		&self,
		module: &tg::Module,
		version: i32,
		text: String,
	) -> tg::Result<()> {
		// Lock the documents.
		let mut documents = self.documents.write().await;

		// Set the state.
		let state = tg::document::State::Opened(tg::document::Opened { version, text });
		documents.insert(module.clone(), state);

		Ok(())
	}

	/// Update a document.
	pub async fn update_document(
		&self,
		document: &tg::Module,
		range: Option<tg::Range>,
		version: i32,
		text: String,
	) -> tg::Result<()> {
		// Lock the documents.
		let mut documents = self.documents.write().await;

		// Get the state.
		let Some(tg::document::State::Opened(state)) = documents.get_mut(document) else {
			return Err(tg::error!("could not find an open document"));
		};

		// Update the version.
		state.version = version;

		// Convert the range to bytes.
		let range = if let Some(range) = range {
			range.to_byte_range_in_string(&state.text)
		} else {
			0..state.text.len()
		};

		// Replace the text.
		state.text.replace_range(range, &text);

		Ok(())
	}

	/// Close a document.
	pub async fn close_document(&self, document: &tg::Module) -> tg::Result<()> {
		// Lock the documents.
		let mut documents = self.documents.write().await;

		// Remove the document.
		documents.remove(document);

		Ok(())
	}

	pub async fn _get_document_version(&self, document: &tg::Module) -> tg::Result<i32> {
		self.try_get_document_version(document)
			.await?
			.ok_or_else(|| tg::error!("expected the document to exist"))
	}

	/// Get a document's version.
	pub async fn try_get_document_version(&self, module: &tg::Module) -> tg::Result<Option<i32>> {
		// Lock the documents.
		let mut documents = self.documents.write().await;

		// Get the state.
		let Some(state) = documents.get_mut(module) else {
			return Ok(None);
		};

		let version = match state {
			tg::document::State::Closed(closed) => {
				let path = module
					.path()
					.ok_or_else(|| tg::error!("expected a module path"))?;

				let metadata = tokio::fs::metadata(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
				let modified = metadata.modified().map_err(|error| {
					tg::error!(source = error, "failed to get the last modification time")
				})?;
				if modified > closed.modified {
					closed.modified = modified;
					closed.version += 1;
				}
				closed.version
			},
			tg::document::State::Opened(opened) => opened.version,
		};

		Ok(Some(version))
	}

	pub async fn get_document_text(&self, module: &tg::Module) -> tg::Result<String> {
		self.try_get_document_text(module)
			.await?
			.ok_or_else(|| tg::error!("expected the document to exist"))
	}

	/// Get a document's text.
	pub async fn try_get_document_text(&self, module: &tg::Module) -> tg::Result<Option<String>> {
		let documents = self.documents.read().await;
		let Some(state) = documents.get(module) else {
			return Ok(None);
		};

		let Some(path) = module.path() else {
			return Ok(None);
		};

		let text = match state {
			tg::document::State::Closed(_) => tokio::fs::read_to_string(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the file"))?,
			tg::document::State::Opened(opened) => opened.text.clone(),
		};
		Ok(Some(text))
	}
}

impl Compiler {
	pub(super) async fn handle_did_open_notification(
		&self,
		params: lsp::DidOpenTextDocumentParams,
	) -> tg::Result<()> {
		// Get the module.
		let module = self.module_for_url(&params.text_document.uri).await?;

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
		let module = self.module_for_url(&params.text_document.uri).await?;

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
		let module = self.module_for_url(&params.text_document.uri).await?;

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
