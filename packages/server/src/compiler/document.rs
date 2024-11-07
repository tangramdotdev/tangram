use super::Compiler;
use lsp_types as lsp;
use tangram_client as tg;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::Module,
}

pub type Response = serde_json::Value;

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
	/// Document a module.
	pub async fn document(&self, module: &tg::Module) -> tg::Result<Response> {
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
			dirty: false,
			version,
			modified: None,
			text: Some(text),
		};

		// Insert the document.
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

		// Mark the document as dirty.
		document.dirty = true;

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

		// Mark the document as clean.
		document.dirty = false;

		// Clear the document's text.
		document.text = None;

		// Set the document's modified time if it is a path module.
		let tg::module::Item::Path(path) = &module.referent.item else {
			return Ok(());
		};
		let path = if let Some(subpath) = &module.referent.subpath {
			path.join(subpath)
		} else {
			path.clone()
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

	// Close a document.
	pub async fn save_document(&self, module: &tg::Module) -> tg::Result<()> {
		// Mark the document as clean.
		let mut document = self
			.documents
			.get_mut(module)
			.ok_or_else(|| tg::error!("failed to get document"))?;
		document.dirty = false;

		// Check in the object if necessary.
		let tg::module::Item::Path(package_path) = module.referent.item.clone() else {
			return Ok(());
		};
		let arg = tg::artifact::checkin::Arg {
			path: package_path.clone(),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
		};
		tg::Artifact::check_in(&self.server, arg).await.map_err(
			|source| tg::error!(!source, %package = package_path.display(), "failed to check in package"),
		)?;
		Ok(())
	}
}

impl Compiler {
	pub(super) async fn handle_did_open_notification(
		&self,
		params: lsp::DidOpenTextDocumentParams,
	) -> tg::Result<()> {
		// Check in the object if necessary.
		if params.text_document.uri.scheme().unwrap().as_str() != "file" {
			return Err(tg::error!(%uri = params.text_document.uri, "expected a file URI"));
		}
		let arg = tg::artifact::checkin::Arg {
			path: params.text_document.uri.path().as_str().into(),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
		};
		tg::Artifact::check_in(&self.server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in package"))?;


		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

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
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

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
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Close the document.
		self.close_document(&module).await?;

		// Update all diagnostics.
		self.update_diagnostics().await?;

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

		// Update all diagnostics.
		self.update_diagnostics().await?;

		Ok(())
	}
}
