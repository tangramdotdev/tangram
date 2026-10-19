use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
}

pub type Output = tg::document::Output;

impl Compiler {
	/// Document a module.
	pub async fn document(&self, module: &tg::module::Data) -> tg::Result<Output> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
		};

		// Get the output.
		let output = self.backend(module.kind).document(self, arg).await?;

		Ok(output)
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

/// Replace the slots in documentation with the indices of the declarations they refer to.
#[cfg(any(feature = "tsrs", feature = "ty"))]
pub(crate) fn resolve(output: &mut tg::document::Output, slots: &[Vec<usize>]) {
	// Resolve the exports, removing the exports without declarations.
	resolve_exports(&mut output.exports, slots);

	// Resolve the declarations.
	for declaration in &mut output.declarations {
		match declaration {
			tg::document::Declaration::Class(class) => {
				for signature in &mut class.constructors {
					resolve_signature(signature, slots);
				}
				for type_ in class.extends.iter_mut().chain(&mut class.implements) {
					resolve_type(type_, slots);
				}
				for property in &mut class.properties {
					resolve_type(&mut property.type_, slots);
				}
				resolve_type_parameters(&mut class.type_parameters, slots);
			},
			tg::document::Declaration::Enum(_) => {},
			tg::document::Declaration::Function(function) => {
				resolve_signature(&mut function.signature, slots);
			},
			tg::document::Declaration::Interface(interface) => {
				for type_ in &mut interface.extends {
					resolve_type(type_, slots);
				}
				resolve_members(&mut interface.members, slots);
				resolve_type_parameters(&mut interface.type_parameters, slots);
			},
			tg::document::Declaration::Namespace(namespace) => {
				resolve_exports(&mut namespace.exports, slots);
			},
			tg::document::Declaration::TypeAlias(alias) => {
				resolve_type(&mut alias.type_, slots);
				resolve_type_parameters(&mut alias.type_parameters, slots);
			},
			tg::document::Declaration::Variable(variable) => {
				resolve_type(&mut variable.type_, slots);
			},
		}
	}
}

#[cfg(any(feature = "tsrs", feature = "ty"))]
fn resolve_exports(
	exports: &mut std::collections::BTreeMap<String, Vec<usize>>,
	slots: &[Vec<usize>],
) {
	for ids in exports.values_mut() {
		resolve_ids(ids, slots);
	}
	exports.retain(|_, ids| !ids.is_empty());
}

#[cfg(any(feature = "tsrs", feature = "ty"))]
fn resolve_ids(ids: &mut Vec<usize>, slots: &[Vec<usize>]) {
	if let Some(slot) = ids.first() {
		*ids = slots[*slot].clone();
	}
}

#[cfg(any(feature = "tsrs", feature = "ty"))]
fn resolve_members(members: &mut tg::document::Members, slots: &[Vec<usize>]) {
	for signature in members
		.call_signatures
		.iter_mut()
		.chain(&mut members.construct_signatures)
	{
		resolve_signature(signature, slots);
	}
	for signature in &mut members.index_signatures {
		resolve_type(&mut signature.key, slots);
		resolve_type(&mut signature.type_, slots);
	}
	for property in &mut members.properties {
		resolve_type(&mut property.type_, slots);
	}
}

#[cfg(any(feature = "tsrs", feature = "ty"))]
fn resolve_signature(signature: &mut tg::document::Signature, slots: &[Vec<usize>]) {
	for parameter in &mut signature.parameters {
		resolve_type(&mut parameter.type_, slots);
	}
	resolve_type(&mut signature.return_, slots);
	resolve_type_parameters(&mut signature.type_parameters, slots);
}

#[cfg(any(feature = "tsrs", feature = "ty"))]
fn resolve_type(type_: &mut tg::document::Type, slots: &[Vec<usize>]) {
	match type_ {
		tg::document::Type::Array(array) => {
			resolve_type(&mut array.type_, slots);
		},
		tg::document::Type::Conditional(conditional) => {
			resolve_type(&mut conditional.check_type, slots);
			resolve_type(&mut conditional.extends_type, slots);
			resolve_type(&mut conditional.false_type, slots);
			resolve_type(&mut conditional.true_type, slots);
		},
		tg::document::Type::Function(function) => {
			for signature in &mut function.signatures {
				resolve_signature(signature, slots);
			}
		},
		tg::document::Type::IndexedAccess(indexed_access) => {
			resolve_type(&mut indexed_access.index_type, slots);
			resolve_type(&mut indexed_access.object_type, slots);
		},
		tg::document::Type::Infer(type_parameter) => {
			resolve_type_parameters(std::slice::from_mut(type_parameter.as_mut()), slots);
		},
		tg::document::Type::Intersection(types) | tg::document::Type::Union(types) => {
			for type_ in &mut types.types {
				resolve_type(type_, slots);
			}
		},
		tg::document::Type::Keyword(_)
		| tg::document::Type::Literal(_)
		| tg::document::Type::Other(_)
		| tg::document::Type::Query(_) => {},
		tg::document::Type::Mapped(mapped) => {
			for type_ in [
				&mut mapped.constraint,
				&mut mapped.name_type,
				&mut mapped.type_,
			]
			.into_iter()
			.flatten()
			{
				resolve_type(type_, slots);
			}
		},
		tg::document::Type::Object(members) => {
			resolve_members(members, slots);
		},
		tg::document::Type::Operator(operator) => {
			resolve_type(&mut operator.type_, slots);
		},
		tg::document::Type::Predicate(predicate) => {
			if let Some(type_) = &mut predicate.type_ {
				resolve_type(type_, slots);
			}
		},
		tg::document::Type::Reference(reference) => {
			resolve_ids(&mut reference.declarations, slots);
			for type_ in &mut reference.type_arguments {
				resolve_type(type_, slots);
			}
		},
		tg::document::Type::TemplateLiteral(template_literal) => {
			for span in &mut template_literal.template_spans {
				resolve_type(&mut span.type_, slots);
			}
		},
		tg::document::Type::Tuple(tuple) => {
			for element in &mut tuple.elements {
				resolve_type(&mut element.type_, slots);
			}
		},
	}
}

#[cfg(any(feature = "tsrs", feature = "ty"))]
fn resolve_type_parameters(
	type_parameters: &mut [tg::document::TypeParameter],
	slots: &[Vec<usize>],
) {
	for type_parameter in type_parameters {
		for type_ in [&mut type_parameter.constraint, &mut type_parameter.default]
			.into_iter()
			.flatten()
		{
			resolve_type(type_, slots);
		}
	}
}
