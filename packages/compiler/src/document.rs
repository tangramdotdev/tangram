use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
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
	pub module: tg::module::Data,
	pub open: bool,
	/// The source revision used by the checkers, independent of the editor version.
	pub revision: u64,
	pub text: Option<String>,
	pub version: i32,
}

/// The source identity shared by all references to an editor document.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Key {
	kind: tg::module::Kind,
	source: tg::module::data::Source,
}

/// The lockfile associated with a document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lockfile {
	pub mtime: std::time::SystemTime,
	pub path: std::path::PathBuf,
}

impl Key {
	#[must_use]
	pub fn new(module: &tg::module::Data) -> Self {
		// A graph pointer and its materialized artifact identify the same source.
		let source = match &module.referent.node {
			tg::module::data::Source::Edge(tg::graph::data::Edge::Pointer(pointer)) => {
				let pointer = tg::graph::Pointer {
					graph: tg::Graph::with_id(pointer.graph.clone()),
					index: pointer.index,
					kind: pointer.kind,
				};
				let id = tg::Artifact::with_pointer(pointer).id();
				tg::module::data::Source::Edge(tg::graph::data::Edge::Object(id.into()))
			},
			source => source.clone(),
		};
		Self {
			kind: module.kind,
			source,
		}
	}
}

impl Compiler {
	/// List the documents.
	pub async fn list_documents(&self) -> Vec<tg::module::Data> {
		self.documents
			.iter()
			.filter(|entry| entry.open)
			.map(|entry| entry.module.clone())
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
		let lockfile = if let Ok(path) = module.referent.node().try_unwrap_path_ref() {
			self.find_lockfile_for_path(path).await
		} else {
			None
		};

		// Advance the source revision even when the editor restarts its version sequence.
		let entry = self.documents.entry(Key::new(module));
		let revision = match &entry {
			dashmap::Entry::Occupied(entry) => entry.get().revision + 1,
			dashmap::Entry::Vacant(_) => 1,
		};

		// Create the document.
		let document = Document {
			dirty: false,
			lockfile,
			modified: None,
			module: module.clone(),
			open: true,
			revision,
			text: Some(text),
			version,
		};

		// Insert the document.
		entry.insert(document);

		Ok(())
	}

	// Save a document.
	pub async fn save_document(&self, module: &tg::module::Data) -> tg::Result<()> {
		// Mark the document as clean.
		let mut document = self
			.documents
			.get_mut(&Key::new(module))
			.ok_or_else(|| tg::error!("failed to get document"))?;
		document.dirty = false;

		Ok(())
	}

	/// Close a document.
	pub async fn close_document(&self, module: &tg::module::Data) -> tg::Result<()> {
		// Get the document.
		let Some(mut document) = self.documents.get_mut(&Key::new(module)) else {
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

		// Switch back to the disk contents and invalidate the cached source text.
		document.text = None;
		document.revision += 1;

		// Set the document's modified time if it is a path module.
		let tg::module::data::Source::Path(path) = &module.referent.node else {
			return Ok(());
		};
		let metadata = match tokio::fs::symlink_metadata(&path).await {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				document.modified = None;
				return Ok(());
			},
			Err(error) => return Err(tg::error!(!error, "failed to get the metadata")),
		};
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
		let Some(mut document) = self.documents.get_mut(&Key::new(&module)) else {
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
		document.revision += 1;

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

		// Request a diagnostics refresh.
		let compiler = self.clone();
		tokio::spawn(async move {
			let result = compiler
				.send_request::<lsp::request::WorkspaceDiagnosticRefresh>(())
				.await;
			if let Err(error) = result {
				tracing::warn!(?error, "failed to refresh diagnostics");
			}
		});

		Ok(())
	}
}
