use {
	super::{Compiler, jsonrpc},
	lsp::notification::Notification as _,
	lsp_types as lsp,
	std::{path::Path, pin::pin},
	tangram_client as tg,
	tangram_futures::stream::TryExt,
	tg::Handle as _,
};

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
	pub dirty: bool,
	pub lockfile: Option<Lockfile>,
	pub modified: Option<std::time::SystemTime>,
	pub open: bool,
	pub text: Option<String>,
	pub version: i32,
}

/// The lockfile associated with a document.
#[derive(Clone, Debug)]
pub struct Lockfile {
	pub path: std::path::PathBuf,
	pub mtime: u64,
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
		// Find the lockfile if this is a path module.
		let lockfile = if let Ok(path) = module.referent.item().try_unwrap_path_ref() {
			self.find_lockfile_for_path(path).await
		} else {
			None
		};

		// Create the document.
		let document = Document {
			open: true,
			dirty: false,
			version,
			modified: None,
			text: Some(text),
			lockfile,
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
			return Err(tg::error!("expected the document to be open"));
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

		// Check in the module in the background if necessary.
		if let Ok(path) = module.referent.item().try_unwrap_path_ref() {
			self.spawn_checkin_task(path);
		}

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
		let encoding = *self.position_encoding.read().unwrap();
		let text = document.text.as_mut().unwrap();
		for change in &params.content_changes {
			let range = if let Some(range) = change.range {
				tg::Range::from(range)
					.try_to_byte_range_in_string(text, encoding)
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

		// Check in the module in the background if necessary.
		if let Ok(path) = module.referent.item().try_unwrap_path_ref() {
			self.spawn_checkin_task(path);
		}

		Ok(())
	}

	fn spawn_checkin_task(&self, path: &Path) {
		let handle = self.handle.clone();
		let sender = self.sender.read().unwrap().clone();
		let compiler = self.clone();
		self.checkin_tasks.spawn(path.to_owned(), |_| {
			let path = path.to_owned();
			async move {
				// Wait 100ms before running checkin.
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;

				// Run checkin.
				let arg = tg::checkin::Arg {
					options: tg::checkin::Options::default(),
					path: path.clone(),
					updates: Vec::new(),
				};
				let result = async {
					let stream = handle.checkin(arg).await?;
					let stream = pin!(stream);
					let output = stream
						.try_last()
						.await?
						.ok_or_else(|| tg::error!("expected an event"))?
						.try_unwrap_output()
						.ok()
						.ok_or_else(|| tg::error!("expected the end event"))?;
					Ok::<_, tg::Error>(output)
				}
				.await;

				// Send a message if the checkin failed.
				let Some(sender) = sender.as_ref() else {
					return;
				};
				if let Err(error) = result {
					let params = lsp::ShowMessageParams {
						typ: lsp::MessageType::ERROR,
						message: format!("checkin failed {error}"),
					};
					let message = jsonrpc::Message::Notification(jsonrpc::Notification {
						jsonrpc: jsonrpc::VERSION.to_owned(),
						method: lsp::notification::ShowMessage::METHOD.to_owned(),
						params: Some(serde_json::to_value(params).unwrap()),
					});
					sender.send(message).ok();
					return;
				}

				// Send a log message if the checkin succeeded.
				let params = lsp::LogMessageParams {
					typ: lsp::MessageType::INFO,
					message: format!("checked in {}", path.display()),
				};
				let message = jsonrpc::Message::Notification(jsonrpc::Notification {
					jsonrpc: jsonrpc::VERSION.to_owned(),
					method: lsp::notification::LogMessage::METHOD.to_owned(),
					params: Some(serde_json::to_value(params).unwrap()),
				});
				sender.send(message).ok();

				// Request to refresh diagnostics.
				tokio::spawn(async move {
					let result = compiler
						.send_request::<lsp::request::WorkspaceDiagnosticRefresh>(())
						.await;
					if let Err(error) = result {
						tracing::warn!(?error, "failed to refresh diagnostics");
					}
				});
			}
		});
	}
}
