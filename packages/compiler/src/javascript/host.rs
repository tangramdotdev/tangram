use {
	super::module::{module_path, try_library_file, try_module_from_path},
	crate::Compiler,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Component, Path, PathBuf},
		sync::Mutex,
		time::SystemTime,
	},
	tangram_client::prelude::*,
	tsrs_lsproto as protocol,
	tsrs_project::FileChangeSummary,
};

pub(super) struct Host {
	pub(super) compiler: Compiler,
	files: Mutex<Files>,

	/// The modules in other languages to load without their extracted interfaces.
	untyped: BTreeSet<crate::document::Key>,
}

pub(super) struct Changes {
	pub(super) files: FileChangeSummary,
	pub(super) resolutions: bool,
	states: BTreeMap<String, SourceState>,
	unresolved: BTreeSet<Import>,
}

#[derive(Default)]
struct Files {
	states: BTreeMap<String, SourceState>,
	texts: BTreeMap<String, String>,
	tokens: BTreeMap<String, tg::authorization::Tokens>,
	unresolved: BTreeSet<Import>,
}

#[derive(Clone, Eq, PartialEq)]
struct SourceState {
	lockfile: Option<crate::document::Lockfile>,
	revision: Option<u64>,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct Import {
	attributes: Option<BTreeMap<String, String>>,
	referrer: String,
	specifier: String,
}

impl Host {
	#[must_use]
	pub(super) fn with_untyped(
		compiler: Compiler,
		untyped: BTreeSet<crate::document::Key>,
	) -> Self {
		Self {
			compiler,
			files: Mutex::default(),
			untyped,
		}
	}

	/// Determine whether to load a module with its extracted interface.
	fn typed(&self, module: &tg::module::Data) -> bool {
		self.untyped.is_empty() || !self.untyped.contains(&crate::document::Key::new(module))
	}

	pub(super) fn try_module(&self, path: &str) -> tg::Result<Option<tg::module::Data>> {
		self.files.lock().unwrap().try_module(path)
	}

	pub(super) fn restore(&self, changes: Changes) {
		let mut files = self.files.lock().unwrap();
		files.states = changes.states;
		files.unresolved = changes.unresolved;
		files.texts.clear();
	}

	pub(super) fn contains_module_identity(&self, text: &str) -> bool {
		self.files.lock().unwrap().texts.keys().any(|path| {
			path.strip_prefix("/__runtime__/")
				.and_then(|path| path.split('/').next())
				.is_some_and(|identity| text.contains(identity))
		})
	}

	pub(super) fn changes(&self) -> tg::Result<Changes> {
		// Compare the same source revisions and lockfile timestamps used by the TypeScript 6 host.
		let states = self.files.lock().unwrap().states.clone();
		let mut files = FileChangeSummary::default();
		let mut resolutions = false;
		for (path, previous) in &states {
			let module = self.files.lock().unwrap().try_module(path)?;
			let Some(module) = module else { continue };
			let current = self.state(&module);
			resolutions |= previous.lockfile != current.lockfile;
			if previous.revision != current.revision {
				self.files.lock().unwrap().texts.remove(path);
				let uri = protocol::DocumentUri(format!("file://{path}"));
				if current.revision.is_none() {
					files.deleted.add(uri);
				} else if previous.revision.is_none() {
					files.created.add(uri);
				} else {
					files.changed.add(uri);
				}
			}
			self.files
				.lock()
				.unwrap()
				.states
				.insert(path.clone(), current);
		}

		// Retry unresolved imports so newly created dependencies join the existing project.
		let unresolved = self.files.lock().unwrap().unresolved.clone();
		for import in &unresolved {
			let uri = protocol::DocumentUri(format!("file://{}", import.referrer));
			if files.changed.has(&uri) || files.deleted.has(&uri) {
				continue;
			}
			if self
				.resolve(
					&import.specifier,
					&import.referrer,
					import.attributes.clone(),
				)?
				.is_some()
			{
				resolutions = true;
			}
		}
		let changes = Changes {
			files,
			resolutions,
			states,
			unresolved,
		};

		Ok(changes)
	}

	fn state(&self, module: &tg::module::Data) -> SourceState {
		let revision = self
			.compiler
			.main_runtime_handle
			.block_on(self.compiler.get_module_version_with_language_inner(
				module,
				tg::module::load::Language::JavaScript,
				self.typed(module),
			))
			.ok();
		let lockfile = self
			.compiler
			.main_runtime_handle
			.block_on(self.compiler.get_module_resolution_version(module));
		SourceState { lockfile, revision }
	}

	pub(super) fn prune(&self, program: &tsrs_compiler::Program, roots: &[String]) {
		let mut paths = roots.iter().cloned().collect::<BTreeSet<_>>();
		let mut imports = BTreeSet::new();
		for &file in program.source_files() {
			paths.insert(file.file_name().to_owned());
			for &specifier in file.imports() {
				let attributes =
					tsrs_module::get_module_resolution_attributes(specifier).map(|_| {
						tsrs_module::module_resolution_attributes(specifier)
							.into_iter()
							.map(|(key, value)| (key.to_owned(), value.to_owned()))
							.collect()
					});
				let import = Import {
					attributes,
					referrer: file.file_name().to_owned(),
					specifier: specifier.text().to_owned(),
				};
				imports.insert(import);
			}
		}
		let mut files = self.files.lock().unwrap();
		files.states.retain(|path, _| paths.contains(path));
		files.texts.retain(|path, _| paths.contains(path));
		files.unresolved.retain(|import| imports.contains(import));
	}

	pub(super) fn path(&self, module: &tg::module::Data) -> String {
		self.files.lock().unwrap().path(module)
	}

	pub(super) fn resolve(
		&self,
		specifier: &str,
		containing_file: &str,
		attributes: Option<BTreeMap<String, String>>,
	) -> tg::Result<Option<String>> {
		// Resolve the imports within the bundled declarations.
		if containing_file.starts_with("/__library__/") {
			let path = Path::new(containing_file).parent().unwrap().join(specifier);
			let mut normalized = PathBuf::new();
			for component in path.components() {
				match component {
					Component::CurDir => {},
					Component::ParentDir => {
						normalized.pop();
					},
					component => normalized.push(component),
				}
			}
			let mut path = normalized.to_string_lossy().into_owned();
			if !path.ends_with(".d.ts") {
				if Path::new(&path)
					.extension()
					.is_some_and(|extension| extension == "ts" || extension == "js")
				{
					path.truncate(path.len() - 3);
				}
				path.push_str(".d.ts");
			}
			return Ok(try_library_file(&path).map(|_| path));
		}

		// Resolve the import through the Tangram instance with its authorization tokens.
		let Some(referrer) = self.files.lock().unwrap().try_module(containing_file)? else {
			return Ok(None);
		};
		let key = Import {
			attributes: attributes.clone(),
			referrer: containing_file.to_owned(),
			specifier: specifier.to_owned(),
		};
		let output = self.compiler.main_runtime_handle.block_on(
			self.compiler
				.resolve_module(&referrer, specifier, attributes),
		);
		let output = match output {
			Ok(output) => output,
			Err(error) => {
				self.files.lock().unwrap().unresolved.insert(key);
				tracing::debug!(%error, "javascript module resolution failed");
				return Ok(None);
			},
		};
		self.files.lock().unwrap().unresolved.remove(&key);
		let path = self.path(&output);

		Ok(Some(path))
	}

	fn try_read(&self, path: &str) -> tg::Result<Option<String>> {
		if let Some(text) = self.files.lock().unwrap().texts.get(path).cloned() {
			return Ok(Some(text));
		}
		let module = self.files.lock().unwrap().try_module(path)?;
		let text = if let Some(file) = try_library_file(path) {
			Some(
				file.contents_utf8()
					.ok_or_else(|| tg::error!("invalid encoding for a library source"))?
					.to_owned(),
			)
		} else if let Some(module) = module {
			let state = self.state(&module);
			self.files
				.lock()
				.unwrap()
				.states
				.insert(path.to_owned(), state);
			Some(self.compiler.main_runtime_handle.block_on(
				self.compiler.load_module_with_language_inner(
					&module,
					Some(tg::module::load::Language::JavaScript),
					self.typed(&module),
				),
			)?)
		} else {
			None
		};
		if let Some(text) = &text {
			self.files
				.lock()
				.unwrap()
				.texts
				.insert(path.to_owned(), text.clone());
		}

		Ok(text)
	}
}

impl Files {
	fn path(&mut self, module: &tg::module::Data) -> String {
		let path = module_path(module);
		if !module.referent.options.tokens.is_empty() {
			self.tokens
				.entry(path.clone())
				.or_default()
				.inherit(&module.referent.options.tokens);
		}

		path
	}

	fn try_module(&self, path: &str) -> tg::Result<Option<tg::module::Data>> {
		let Some(mut module) = try_module_from_path(path)? else {
			return Ok(None);
		};
		if let Some(tokens) = self.tokens.get(&module_path(&module)) {
			module.referent.options.tokens = tokens.clone();
		}

		Ok(Some(module))
	}
}

impl tsrs_vfs::FS for Host {
	fn use_case_sensitive_file_names(&self) -> bool {
		true
	}
	fn file_exists(&self, path: &str) -> bool {
		try_library_file(path).is_some()
			|| (path.starts_with("/__runtime__/")
				&& try_module_from_path(path).ok().flatten().is_some())
	}
	fn read_file(&self, path: &str) -> Option<String> {
		match self.try_read(path) {
			Ok(text) => text,
			Err(error) => {
				tracing::debug!(%error, "javascript module loading failed");
				None
			},
		}
	}
	fn write_file(&self, _path: &str, _data: &str) -> Result<(), String> {
		Err("the javascript filesystem is read-only".into())
	}
	fn append_file(&self, _path: &str, _data: &str) -> Result<(), String> {
		Err("the javascript filesystem is read-only".into())
	}
	fn remove(&self, _path: &str) -> Result<(), String> {
		Err("the javascript filesystem is read-only".into())
	}
	fn chtimes(
		&self,
		_path: &str,
		_access: SystemTime,
		_modification: SystemTime,
	) -> Result<(), String> {
		Err("the javascript filesystem is read-only".into())
	}
	fn directory_exists(&self, path: &str) -> bool {
		path == "/" || path.starts_with("/__library__") || path.starts_with("/__runtime__")
	}
	fn get_accessible_entries(&self, _path: &str) -> tsrs_vfs::Entries {
		tsrs_vfs::Entries::default()
	}
	fn stat(&self, path: &str) -> Option<tsrs_vfs::FileInfo> {
		if !self.file_exists(path) && !self.directory_exists(path) {
			return None;
		}
		let mode = if self.directory_exists(path) && !self.file_exists(path) {
			tsrs_vfs::FileMode::Dir
		} else {
			tsrs_vfs::FileMode::None
		};
		Some(tsrs_vfs::FileInfo {
			mod_time: None,
			mode,
			name: Path::new(path).file_name()?.to_str()?.to_owned(),
			size: 0,
		})
	}
	fn realpath(&self, path: &str) -> String {
		path.into()
	}
}
