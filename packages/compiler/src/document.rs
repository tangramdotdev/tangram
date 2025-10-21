use {super::Compiler, lsp_types as lsp, tangram_client as tg};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::module::Data,
}

pub type Response = serde_json::Value;

impl Compiler {
	/// Document a module.
	pub async fn document(&self, module: &tg::module::Data) -> tg::Result<Response> {
		// Create the request.
		let request = super::Request::Document(Request {
			module: module.clone(),
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Document(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response)
	}
}

/// A document.
#[derive(Clone, Debug)]
pub struct Document {
	pub open: bool,
	pub dirty: bool,
	pub version: i32,
	pub modified: Option<std::time::SystemTime>,
	pub text: Option<String>,
}

impl Compiler {
	/// List the documents.
	pub async fn list_documents(&self) -> Vec<tg::module::Data> {
		self.documents
			.iter()
			.filter(|entry| entry.open)
			.map(|entry| entry.key().clone())
			.collect()
	}

	/// Open a document.
	pub async fn open_document(
		&self,
		module: &tg::module::Data,
		version: i32,
		text: String,
	) -> tg::Result<()> {
		let document = Document {
			open: true,
			dirty: false,
			version,
			modified: None,
			text: Some(text),
		};

		// Insert the document.
		self.documents.insert(module.clone(), document);

		Ok(())
	}

	// Save a document.
	pub async fn save_document(&self, module: &tg::module::Data) -> tg::Result<()> {
		// Mark the document as clean.
		let mut document = self
			.documents
			.get_mut(module)
			.ok_or_else(|| tg::error!("failed to get document"))?;
		document.dirty = false;

		Ok(())
	}

	/// Close a document.
	pub async fn close_document(&self, module: &tg::module::Data) -> tg::Result<()> {
		// Get the document.
		let Some(mut document) = self.documents.get_mut(module) else {
			return Err(tg::error!("failed to find the document"));
		};

		// Ensure the document is open.
		if !document.open {
			return Err(tg::error!("expected the document to open"));
		}

		// Mark the document as closed.
		document.open = false;

		// Mark the document as clean.
		document.dirty = false;

		// Clear the document's text.
		document.text = None;

		// Set the document's modified time if it is a path module.
		let tg::module::data::Item::Path(path) = &module.referent.item else {
			return Ok(());
		};
		let metadata = tokio::fs::symlink_metadata(&path)
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
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Open the document.
		let version = params.text_document.version;
		let text = params.text_document.text;
		self.open_document(&module, version, text).await?;

		Ok(())
	}

	pub(super) async fn handle_did_change_notification(
		&self,
		params: lsp::DidChangeTextDocumentParams,
	) -> tg::Result<()> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the document.
		let Some(mut document) = self.documents.get_mut(&module) else {
			return Err(tg::error!("failed to find the document"));
		};

		// Ensure it is open.
		if !document.open {
			return Err(tg::error!("expected the document to be open"));
		}

		// Mark it dirty.
		document.dirty = true;

		// Apply the changes.
		let text = document.text.as_mut().unwrap();
		for change in &params.content_changes {
			let range = if let Some(range) = change.range {
				tg::Range::from(range)
					.try_to_byte_range_in_string(text)
					.ok_or_else(|| tg::error!("invalid range"))?
			} else {
				0..text.len()
			};
			text.replace_range(range, &change.text);
		}

		// Set the version.
		document.version = params.text_document.version;

		// Drop the document.
		drop(document);

		Ok(())
	}

	pub(super) async fn handle_did_close_notification(
		&self,
		params: lsp::DidCloseTextDocumentParams,
	) -> tg::Result<()> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Close the document.
		self.close_document(&module).await?;

		Ok(())
	}

	pub(super) async fn handle_did_save_notification(
		&self,
		params: lsp::DidSaveTextDocumentParams,
	) -> tg::Result<()> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Save the module.
		self.save_document(&module).await?;

		Ok(())
	}
}
