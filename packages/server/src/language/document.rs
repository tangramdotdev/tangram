use super::{Sender, Server};
use lsp_types as lsp;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};

impl Server {
	/// Get all the server's documents.
	pub(crate) async fn get_documents(&self) -> Vec<tg::Document> {
		let documents = self.inner.documents.read().await;
		documents.keys().cloned().collect()
	}

	/// Get a document.
	pub(crate) async fn get_document(
		&self,
		package_path: PathBuf,
		module_path: tg::Path,
	) -> Result<tg::Document> {
		let path = package_path.join(module_path.to_string());

		// Create the document.
		let document = tg::Document {
			package_path,
			path: module_path,
		};

		// Lock the documents.
		let mut documents = self.inner.documents.write().await;

		// Add the document to the store if it is not present.
		if !documents.contains_key(&document) {
			let metadata = tokio::fs::metadata(&path)
				.await
				.wrap_err("Failed to get the metadata.")?;
			let modified = metadata
				.modified()
				.wrap_err("Failed to get the last modification time.")?;
			let state = tg::document::State::Closed(tg::document::Closed {
				version: 0,
				modified,
			});
			documents.insert(document.clone(), state);
		}

		Ok(document)
	}

	/// Open a document.
	pub(crate) async fn open_document(
		&self,
		document: &tg::Document,
		version: i32,
		text: String,
	) -> Result<()> {
		// Lock the documents.
		let mut documents = self.inner.documents.write().await;

		// Set the state.
		let state = tg::document::State::Opened(tg::document::Opened { version, text });
		documents.insert(document.clone(), state);

		Ok(())
	}

	/// Update a document.
	pub(crate) async fn update_document(
		&self,
		document: &tg::Document,
		range: Option<tg::Range>,
		version: i32,
		text: String,
	) -> Result<()> {
		// Lock the documents.
		let mut documents = self.inner.documents.write().await;

		// Get the state.
		let Some(tg::document::State::Opened(state)) = documents.get_mut(document) else {
			let path = document.path();
			let path = path.display();
			return Err(error!(
				r#"Could not find an open document for the path "{path}"."#
			));
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
	pub async fn close_document(&self, document: &tg::Document) -> Result<()> {
		// Lock the documents.
		let mut documents = self.inner.documents.write().await;

		// Remove the document.
		documents.remove(document);

		Ok(())
	}

	/// Get a document's version.
	pub async fn get_document_version(&self, document: &tg::Document) -> Result<i32> {
		// Lock the documents.
		let mut documents = self.inner.documents.write().await;

		// Get the state.
		let state = documents.get_mut(document).unwrap();

		let version = match state {
			tg::document::State::Closed(closed) => {
				let metadata = tokio::fs::metadata(document.path())
					.await
					.wrap_err("Failed to get the metadata.")?;
				let modified = metadata
					.modified()
					.wrap_err("Failed to get the last modification time.")?;
				if modified > closed.modified {
					closed.modified = modified;
					closed.version += 1;
				}
				closed.version
			},
			tg::document::State::Opened(opened) => opened.version,
		};

		Ok(version)
	}

	/// Get the document's text.
	pub async fn get_document_text(&self, document: &tg::Document) -> Result<String> {
		let path = document.path();
		let documents = self.inner.documents.read().await;
		let document = documents.get(document).unwrap();
		let text = match document {
			tg::document::State::Closed(_) => tokio::fs::read_to_string(&path)
				.await
				.wrap_err("Failed to read the file.")?,
			tg::document::State::Opened(opened) => opened.text.clone(),
		};
		Ok(text)
	}
}

impl Server {
	pub(super) async fn handle_did_open_notification(
		&self,
		sender: Sender,
		params: lsp::DidOpenTextDocumentParams,
	) -> Result<()> {
		// Get the module.
		let module = self.module_for_url(&params.text_document.uri).await?;

		// Open the document.
		if let tg::Module::Document(document) = &module {
			let version = params.text_document.version;
			let text = params.text_document.text;
			self.open_document(document, version, text).await?;
		}

		// Update all diagnostics.
		self.update_diagnostics(&sender).await?;

		Ok(())
	}

	pub(super) async fn handle_did_change_notification(
		&self,
		sender: Sender,
		params: lsp::DidChangeTextDocumentParams,
	) -> Result<()> {
		// Get the module.
		let module = self.module_for_url(&params.text_document.uri).await?;

		if let tg::Module::Document(document) = &module {
			// Apply the changes.
			for change in params.content_changes {
				self.update_document(
					document,
					change.range.map(Into::into),
					params.text_document.version,
					change.text,
				)
				.await?;
			}
		}

		// Update all diagnostics.
		self.update_diagnostics(&sender).await?;

		Ok(())
	}

	pub(super) async fn handle_did_close_notification(
		&self,
		sender: Sender,
		params: lsp::DidCloseTextDocumentParams,
	) -> Result<()> {
		// Get the module.
		let module = self.module_for_url(&params.text_document.uri).await?;

		if let tg::Module::Document(document) = &module {
			// Close the document.
			self.close_document(document).await?;
		}

		// Update all diagnostics.
		self.update_diagnostics(&sender).await?;

		Ok(())
	}

	pub(super) async fn handle_did_save_notification(
		&self,
		sender: Sender,
		_params: lsp::DidSaveTextDocumentParams,
	) -> Result<()> {
		// Update all diagnostics.
		self.update_diagnostics(&sender).await?;

		Ok(())
	}
}
