use super::{Sender, Server, ROOT_MODULE_FILE_NAME};
use crate::{Module, Range};
use lsp_types as lsp;
use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::Arc,
	time::SystemTime,
};
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};

/// A document.
#[derive(
	Clone, PartialOrd, Ord, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct Document {
	/// The path to the package.
	pub package_path: PathBuf,

	/// The module's path.
	pub path: tg::Path,
}

/// A document's state.
#[derive(Clone, Debug)]
pub enum State {
	/// A closed document.
	Closed(Closed),

	/// An opened document.
	Opened(Opened),
}

/// A closed document.
#[derive(Clone, Debug)]
pub struct Closed {
	/// The document's version.
	pub version: i32,

	/// The document's last modified time.
	pub modified: SystemTime,
}

/// An opened document.
#[derive(Clone, Debug)]
pub struct Opened {
	/// The document's version.
	pub version: i32,

	/// The document's text.
	pub text: String,
}

/// A document store.
#[derive(Clone, Debug, Default)]
pub struct Store(Arc<tokio::sync::RwLock<HashMap<Document, State, fnv::FnvBuildHasher>>>);

impl Server {
	pub(super) async fn handle_did_open_notification(
		&self,
		sender: Sender,
		params: lsp::DidOpenTextDocumentParams,
	) -> Result<()> {
		// Get the module.
		let module = self.module_for_url(&params.text_document.uri).await?;

		// Open the document.
		if let Module::Document(document) = module {
			let version = params.text_document.version;
			let text = params.text_document.text;
			document
				.open(&self.inner.document_store, version, text)
				.await?;
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

		if let Module::Document(document) = module {
			// Apply the changes.
			for change in params.content_changes {
				document
					.update(
						&self.inner.document_store,
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

		if let Module::Document(document) = module {
			// Close the document.
			document.close(&self.inner.document_store).await?;
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

impl Document {
	pub async fn new(store: &Store, package_path: PathBuf, module_path: tg::Path) -> Result<Self> {
		let path = package_path.join(module_path.to_string());

		// Create the document.
		let document = Self {
			package_path,
			path: module_path,
		};

		// Lock the documents.
		let mut documents = store.0.write().await;

		// Set the state to unopened if it is not present.
		if !documents.contains_key(&document) {
			let metadata = tokio::fs::metadata(&path)
				.await
				.wrap_err("Failed to get the metadata.")?;
			let modified = metadata
				.modified()
				.wrap_err("Failed to get the last modification time.")?;
			let state = State::Closed(Closed {
				version: 0,
				modified,
			});
			documents.insert(document.clone(), state);
		}

		Ok(document)
	}

	pub async fn for_path(store: &Store, path: &Path) -> Result<Self> {
		// Find the package path by searching the path's ancestors for a root module.
		let mut found = false;
		let mut package_path = path.to_owned();
		while package_path.pop() {
			if tokio::fs::try_exists(&package_path.join(ROOT_MODULE_FILE_NAME))
				.await
				.wrap_err("Failed to determine if the path exists.")?
			{
				found = true;
				break;
			}
		}
		if !found {
			let path = path.display();
			return Err(error!(r#"Could not find the package for path "{path}"."#));
		}

		// Get the module path by stripping the package path.
		let module_path: tg::Path = path
			.strip_prefix(&package_path)
			.unwrap()
			.to_owned()
			.into_os_string()
			.into_string()
			.ok()
			.wrap_err("The module path was not valid UTF-8.")?
			.parse()
			.wrap_err("Failed to parse the module path.")?;

		// Create the document.
		let document = Self::new(store, package_path, module_path).await?;

		Ok(document)
	}

	/// Open a document.
	pub async fn open(&self, store: &Store, version: i32, text: String) -> Result<()> {
		// Lock the documents.
		let mut documents = store.0.write().await;

		// Set the state.
		let state = State::Opened(Opened { version, text });
		documents.insert(self.clone(), state);

		Ok(())
	}

	/// Update a document.
	pub async fn update(
		&self,
		store: &Store,
		range: Option<Range>,
		version: i32,
		text: String,
	) -> Result<()> {
		// Lock the documents.
		let mut documents = store.0.write().await;

		// Get the state.
		let Some(State::Opened(state)) = documents.get_mut(self) else {
			let path = self.path();
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
	pub async fn close(self, store: &Store) -> Result<()> {
		// Lock the documents.
		let mut documents = store.0.write().await;

		// Remove the document.
		documents.remove(&self);

		Ok(())
	}

	/// Get the document's path.
	#[must_use]
	pub fn path(&self) -> PathBuf {
		self.package_path.join(self.path.to_string())
	}

	/// Get the document's version.
	pub async fn version(&self, store: &Store) -> Result<i32> {
		// Lock the documents.
		let mut documents = store.0.write().await;

		// Get the state.
		let state = documents.get_mut(self).unwrap();

		let version = match state {
			State::Closed(closed) => {
				let metadata = tokio::fs::metadata(self.path())
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
			State::Opened(opened) => opened.version,
		};

		Ok(version)
	}

	/// Get the document's text.
	pub async fn text(&self, store: &Store) -> Result<String> {
		let path = self.path();
		let documents = store.0.read().await;
		let document = documents.get(self).unwrap();
		let text = match document {
			State::Closed(_) => tokio::fs::read_to_string(&path)
				.await
				.wrap_err("Failed to read the file.")?,
			State::Opened(opened) => opened.text.clone(),
		};
		Ok(text)
	}
}

impl Store {
	pub async fn documents(&self) -> Vec<Document> {
		let documents = self.0.read().await;
		documents.keys().cloned().collect()
	}
}
