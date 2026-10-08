use {
	super::{Documents, library, resolve, system::System},
	crate::{Compiler, analyze::python::metadata, document::Key},
	ruff_db::{
		diagnostic::{Diagnostic, Severity, UnifiedFile},
		files::{File, Files, system_path_to_file, vendored_path_to_file},
		source::source_text,
		system::SystemPathBuf,
		vendored::VendoredFileSystem,
	},
	salsa::Setter as _,
	std::{
		borrow::Cow,
		collections::{BTreeMap, BTreeSet, HashMap},
		fmt::Write as _,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	ty_module_resolver::{
		FallibleStrategy, ImportingFile, Module, ModuleKind, ModuleName, ModuleResolution,
		ModuleResolveMode, ResolverEnvironment, ResolverFile, SearchPathSettings,
	},
	ty_python_ast::PythonVersion,
	ty_python_core::{Program, ProgramFile, program::ProgramSettings},
	ty_python_semantic::{
		AnalysisSettings, PythonVersionWithSource,
		dependency::DependencyMetadata,
		lint::{LintRegistry, RuleSelection},
	},
};

mod completion;
mod diagnostics;
mod query;
mod symbols;
mod tokens;

#[salsa::db]
#[derive(Clone)]
// The worker updates Salsa inputs between requests and retains query results across requests.
pub(super) struct Database {
	analysis: Arc<AnalysisSettings>,
	compiler: Compiler,
	documents: Arc<Documents>,
	error: Arc<Mutex<Option<tg::Error>>>,
	files: Files,
	modules: Arc<Mutex<Modules>>,
	project: Option<ty_project::Project>,
	project_paths: Vec<SystemPathBuf>,
	resolutions: Arc<Mutex<HashMap<File, BTreeMap<String, Resolution>>>>,
	revision: Option<Revision>,
	rules: Arc<RuleSelection>,
	settings: Arc<ProgramSettings>,
	storage: salsa::Storage<Self>,
	system: System,
	uv_environments: ty_project::UvEnvironments,
}

#[derive(Default)]
struct Modules {
	children: BTreeMap<(String, String), resolve::Target>,
	entries: Vec<Arc<Entry>>,
	files: HashMap<File, usize>,
	keys: BTreeMap<String, usize>,
	links: HashMap<File, BTreeSet<String>>,
	namespaces: BTreeMap<String, (ModuleName, Box<resolve::Namespace>)>,
}

#[derive(Clone)]
struct Entry {
	diagnostics: Vec<tg::Diagnostic>,
	error: Option<tg::Error>,
	file: File,
	imports: BTreeMap<String, tg::module::Import>,
	module: tg::module::Data,
	name: ModuleName,
	package: bool,
	text: String,
	version: Option<u64>,
}

#[derive(Clone)]
struct Resolution {
	output: resolve::Output,
	request: resolve::Request,
}

#[salsa::input]
struct Revision {
	value: u64,
}

impl Database {
	pub(super) fn new(compiler: Compiler, documents: Documents) -> tg::Result<Self> {
		let system = System::default();
		library::load(&system)?;
		let mut search_paths = SearchPathSettings::empty();
		search_paths
			.extra_paths
			.push(SystemPathBuf::from("/library"));
		let mut settings = ProgramSettings::empty(ty_vendored::file_system());
		settings.python_version.version = PythonVersion::PY314;
		settings.search_paths = search_paths
			.to_search_paths(&system, ty_vendored::file_system(), &FallibleStrategy)
			.map_err(|error| tg::error!(!error, "failed to configure the python library"))?;
		let mut db = Self {
			analysis: Arc::new(AnalysisSettings::default()),
			compiler,
			documents: Arc::new(documents),
			error: Arc::default(),
			files: Files::default(),
			modules: Arc::default(),
			project: None,
			project_paths: Vec::new(),
			resolutions: Arc::default(),
			revision: None,
			rules: Arc::new(RuleSelection::from_registry(
				ty_python_semantic::default_lint_registry(),
			)),
			settings: Arc::new(settings),
			storage: salsa::Storage::default(),
			system,
			uv_environments: ty_project::UvEnvironments::default(),
		};
		// The project contains only explicitly supplied module files, including when that set is empty.
		db.system
			.memory
			.create_directory_all("/project")
			.map_err(|error| tg::error!(!error, "failed to create the python project directory"))?;
		let metadata = ty_project::ProjectMetadata::new("tangram", "/project".into());
		let (settings, diagnostics) = metadata
			.to_merged_options()
			.to_settings(&db, &FallibleStrategy)
			.map_err(|error| tg::error!(!error, "failed to configure the python project"))?;
		let project = ty_project::Project::builder(
			Box::new(metadata),
			Box::new(settings),
			(*db.settings).clone(),
			diagnostics,
		)
		.new(&db);
		db.project = Some(project);
		db.revision = Some(Revision::new(&db, 0));
		tracing::debug!("created the Python database");
		Ok(db)
	}

	pub(super) fn update(&mut self, documents: Documents) -> tg::Result<()> {
		// Capture the editor state before loading any sources or resolving imports.
		let mut changed = self.documents.keys().ne(documents.keys());
		let previous_documents = std::mem::replace(&mut self.documents, Arc::new(documents));
		self.error.lock().unwrap().take();
		let entries = self.modules.lock().unwrap().entries.clone();
		for entry in entries {
			let key = Key::new(&entry.module);
			if !matches!(
				entry.module.referent.node,
				tg::module::data::Source::Path(_)
			) && !self.documents.contains_key(&key)
				&& !previous_documents.contains_key(&key)
			{
				continue;
			}
			let version = self.version(&entry.module);
			if version.is_some()
				&& version == entry.version
				&& !self.documents.contains_key(&key)
				&& !previous_documents.contains_key(&key)
			{
				continue;
			}
			let missing = match &entry.module.referent.node {
				tg::module::data::Source::Path(path) => {
					!self.documents.contains_key(&key) && !path.exists()
				},
				tg::module::data::Source::Edge(_) => false,
			};
			let (text, error) = if missing {
				(
					String::new(),
					Some(tg::error!("the python source file does not exist")),
				)
			} else {
				match self.load(&entry.module) {
					Ok(text) => (text, None),
					Err(error) => (String::new(), Some(error)),
				}
			};
			let path = entry.file.path(self).as_system_path().unwrap().to_owned();
			let exists = self.system.memory.metadata(&path).is_ok();
			if text == entry.text
				&& exists != missing
				&& error.as_ref().map(ToString::to_string)
					== entry.error.as_ref().map(ToString::to_string)
			{
				let mut updated = (*entry).clone();
				updated.version = version;
				let mut modules = self.modules.lock().unwrap();
				let index = modules.files[&entry.file];
				modules.entries[index] = Arc::new(updated);
				continue;
			}
			let mut updated = (*entry).clone();
			updated.version = version;
			updated.text = text;
			updated.error = error;
			let (imports, diagnostics) = Self::metadata(&entry.module, &updated.text)?;
			updated.imports = imports;
			updated.diagnostics = diagnostics;
			if missing {
				self.system.memory.remove_file(&path).ok();
			} else {
				self.system
					.memory
					.write_file(&path, &updated.text)
					.map_err(|error| tg::error!(!error, "failed to update the python source"))?;
			}
			let index = self.modules.lock().unwrap().files[&entry.file];
			self.modules.lock().unwrap().entries[index] = Arc::new(updated);
			self.resolutions.lock().unwrap().remove(&entry.file);
			self.modules.lock().unwrap().links.remove(&entry.file);
			entry.file.sync(self);
			changed = true;
		}

		// Revalidate external resolution inputs, including missing files and moved tags.
		let resolutions = self.resolutions.lock().unwrap().clone();
		for (file, records) in resolutions {
			for (key, mut record) in records {
				let output = self.resolve_inner(record.request.clone());
				if output.without_token() != record.output.without_token() {
					changed = true;
				}
				// Retain refreshed authorization even when the resolution identity is unchanged.
				record.output = output;
				self.resolutions
					.lock()
					.unwrap()
					.entry(file)
					.or_default()
					.insert(key, record);
			}
		}
		if changed {
			self.resolutions.lock().unwrap().clear();
			self.modules.lock().unwrap().links.clear();
			self.modules.lock().unwrap().children.clear();
			let revision = self.revision.unwrap();
			let value = revision.value(self) + 1;
			revision.set_value(self).to(value);
		}
		Ok(())
	}

	fn version(&self, module: &tg::module::Data) -> Option<u64> {
		if let Some(document) = self.documents.get(&Key::new(module)) {
			return Some(document.revision);
		}
		self.compiler
			.main_runtime_handle
			.block_on(self.compiler.get_module_version(module))
			.ok()
	}

	fn load(&self, module: &tg::module::Data) -> tg::Result<String> {
		if let Some(document) = self.documents.get(&Key::new(module)) {
			return crate::load::module(
				module,
				document.text.as_ref().unwrap(),
				Some(tg::module::load::Language::Python),
			);
		}
		let arg = tg::module::load::Arg {
			language: Some(tg::module::load::Language::Python),
			module: module.clone(),
		};
		let text = self
			.compiler
			.main_runtime_handle
			.block_on(self.compiler.instance.load_module(arg))
			.map_err(|error| tg::error!(!error, %module, "failed to load the python module"))?
			.text;
		Ok(text)
	}

	fn metadata(
		module: &tg::module::Data,
		text: &str,
	) -> tg::Result<(BTreeMap<String, tg::module::Import>, Vec<tg::Diagnostic>)> {
		let descriptor = resolve::Module::new(module.clone());
		// Preserve dependency declarations while the editor contains incomplete Python syntax.
		let output = match metadata::parse_unchecked(&descriptor.filename, text) {
			Ok(metadata) => (metadata.imports, Vec::new()),
			Err(error) => (
				BTreeMap::new(),
				vec![Self::metadata_diagnostic(module, error)?],
			),
		};
		Ok(output)
	}

	fn library_file(&self, module: &tg::module::Data) -> tg::Result<Option<File>> {
		let tg::module::data::Source::Path(path) = &module.referent.node else {
			return Ok(None);
		};
		let root = self.compiler.library_path.join("python");
		let file = if let Ok(path) = path.strip_prefix(root.join("client")) {
			let path = SystemPathBuf::from(format!("/library/{}", path.display()));
			system_path_to_file(self, &path)
		} else if let Ok(path) = path.strip_prefix(root.join("typeshed")) {
			vendored_path_to_file(
				self,
				path.to_str()
					.ok_or_else(|| tg::error!("invalid python library path"))?,
			)
		} else {
			return Ok(None);
		};
		file.map(Some)
			.map_err(|error| tg::error!(!error, "failed to get the python library file"))
	}

	fn location(
		&self,
		file: File,
		range: ty_text_size::TextRange,
		encoding: tg::position::Encoding,
	) -> tg::Result<tg::module::data::Location> {
		let text = source_text(self, file);
		let module = if let Some(entry) = self.entry(file) {
			entry.module.without_token()
		} else {
			let path = file.path(self);
			let path = if let Some(path) = path
				.as_system_path()
				.and_then(|path| path.strip_prefix("/library").ok())
			{
				self.compiler
					.library_path
					.join("python/client")
					.join(path.as_str())
			} else if let Some(path) = path.as_vendored_path() {
				self.compiler
					.library_path
					.join("python/typeshed")
					.join(path.as_str())
			} else {
				return Err(tg::error!("unknown python definition file"));
			};
			std::fs::create_dir_all(path.parent().unwrap()).map_err(|error| {
				tg::error!(!error, "failed to create the python library directory")
			})?;
			std::fs::write(&path, &*text).map_err(|error| {
				tg::error!(!error, "failed to materialize the python library file")
			})?;
			tg::module::Data {
				kind: tg::module::Kind::Python,
				referent: tg::Referent::with_node(tg::module::data::Source::Path(path)),
			}
		};
		let range = tg::Range::try_from_byte_range_in_string(
			&text,
			usize::from(range.start())..usize::from(range.end()),
			encoding,
		)
		.ok_or_else(|| tg::error!("invalid python definition range"))?;
		Ok(tg::module::data::Location { module, range })
	}

	pub(super) fn check(&self, modules: Vec<tg::module::Data>) -> tg::Result<Vec<tg::Diagnostic>> {
		let mut pending = modules
			.into_iter()
			.map(|module| self.register(module))
			.collect::<tg::Result<Vec<_>>>()?;
		let mut checked = BTreeSet::new();
		let mut diagnostics = Vec::new();
		let mut modules = Vec::new();
		while let Some(entry) = pending.pop() {
			if !checked.insert(entry.module.without_token().to_string()) {
				continue;
			}
			if let Some(error) = &entry.error {
				return Err(error.clone());
			}
			modules.push(entry.module.clone());
			diagnostics.extend(entry.diagnostics.iter().cloned());
			if entry.module.kind != tg::module::Kind::Python {
				continue;
			}
			self.register_package(&entry)?;
			let program = Program::from_settings(self, &self.settings);
			let file = program.program_file(self, entry.file);
			let results = ty_python_semantic::check_file(self, file)
				.unwrap_or_else(|error| vec![error].into_boxed_slice());
			for diagnostic in &results {
				diagnostics.push(self.diagnostic(diagnostic, tg::position::Encoding::Utf8)?);
			}
			pending.extend(self.dependencies(entry.file));
		}
		if let Some(error) = self.error.lock().unwrap().take() {
			return Err(error);
		}

		// Warn about the exports of the checked modules.
		let warnings = self.compiler.main_runtime_handle.block_on(
			self.compiler
				.get_export_diagnostics(&modules, tg::position::Encoding::Utf8),
		)?;
		diagnostics.extend(warnings);

		Ok(diagnostics)
	}

	fn register(&self, module: tg::module::Data) -> tg::Result<Arc<Entry>> {
		let module = self
			.compiler
			.main_runtime_handle
			.block_on(resolve::prepare_module(&self.compiler.instance, module))?;
		let key = module.without_token().to_string();
		{
			let modules = self.modules.lock().unwrap();
			if let Some(index) = modules.keys.get(&key) {
				let entry = modules.entries[*index].clone();
				if let Some(error) = &entry.error {
					return Err(error.clone());
				}
				return Ok(entry);
			}
		}
		let version = self.version(&module);
		let (text, error) = match self.load(&module) {
			Ok(text) => (text, None),
			Err(error) => (String::new(), Some(error)),
		};
		let package = resolve::Module::new(module.clone()).package;
		let (imports, diagnostics) = Self::metadata(&module, &text)?;
		let mut modules = self.modules.lock().unwrap();
		if let Some(index) = modules.keys.get(&key) {
			let entry = modules.entries[*index].clone();
			if let Some(error) = &entry.error {
				return Err(error.clone());
			}
			return Ok(entry);
		}
		let index = modules.entries.len();
		let name = ModuleName::new(&format!("m{index}")).unwrap();
		let filename = if package { "__init__.py" } else { "module.py" };
		let directory = if matches!(module.referent.node, tg::module::data::Source::Path(_)) {
			"workspace"
		} else {
			"modules"
		};
		let path = SystemPathBuf::from(format!("/{directory}/{index}/{filename}"));
		self.system
			.memory
			.create_directory_all(path.parent().unwrap())
			.map_err(|error| tg::error!(!error, "failed to create the python module directory"))?;
		self.system
			.memory
			.write_file(&path, &text)
			.map_err(|error| tg::error!(!error, "failed to load the python module source"))?;
		let file = system_path_to_file(self, &path)
			.map_err(|error| tg::error!(!error, "failed to create the python source file"))?;
		let entry = Arc::new(Entry {
			diagnostics,
			error,
			file,
			imports,
			module,
			name,
			package,
			text,
			version,
		});
		modules.files.insert(file, index);
		modules.keys.insert(key, index);
		modules.entries.push(entry.clone());
		drop(modules);
		if let Some(error) = &entry.error {
			return Err(error.clone());
		}
		self.register_package(&entry)?;
		Ok(entry)
	}

	fn register_package(&self, entry: &Entry) -> tg::Result<()> {
		// The runtime loads containing package initializers before executing a module.
		let request = resolve::Request::Package {
			module: entry.module.clone(),
			parent: entry.package,
		};
		if entry.module.kind == tg::module::Kind::Python
			&& let resolve::Output::Resolved(resolution) = self.resolve(entry.file, request)
		{
			self.register_target(&resolution.target)?;
		}
		Ok(())
	}

	fn metadata_diagnostic(
		module: &tg::module::Data,
		error: tg::Error,
	) -> tg::Result<tg::Diagnostic> {
		let tg::Either::Left(data) = error.to_data_or_id() else {
			return Err(error);
		};
		let Some(location) = data.location else {
			return Err(error);
		};
		let location = tg::module::data::Location {
			module: module.without_token(),
			range: location.range,
		};
		let mut message = error.to_string();
		let mut source = std::error::Error::source(&error);
		while let Some(error) = source {
			write!(message, "\n{error}").unwrap();
			source = error.source();
		}
		let diagnostic = tg::diagnostic::Data {
			location: Some(location),
			message,
			severity: tg::diagnostic::Severity::Error,
		};
		diagnostic.try_into()
	}

	#[must_use]
	fn module_display_name<'db>(&'db self, module: Module<'db>) -> Option<Cow<'db, str>> {
		self.revision.unwrap().value(self);
		if let Some(entry) = module.file(self).and_then(|file| self.entry(file)) {
			if matches!(
				entry.module.referent.node,
				tg::module::data::Source::Edge(_)
			) && entry.module.referent.path().is_none()
			{
				return Some(Cow::Owned(entry.module.without_token().to_string()));
			}
			let descriptor = resolve::Module::new(entry.module.clone());
			return Some(Cow::Owned(descriptor.filename.display().to_string()));
		}
		let modules = self.modules.lock().unwrap();
		let (_, namespace) = modules
			.namespaces
			.values()
			.find(|(name, _)| name == module.name(self))?;
		let descriptor = resolve::Module::new(namespace.referrer.clone());
		let directory = if namespace.referrer.kind == tg::module::Kind::Directory {
			descriptor.filename
		} else {
			descriptor.filename.parent()?.to_owned()
		};
		let directory = directory.join(&namespace.prefix);
		let directory = tangram_util::path::normalize(directory);
		Some(Cow::Owned(directory.display().to_string()))
	}

	fn resolve_import_member<'db>(
		&'db self,
		importing_file: ImportingFile<'db>,
		import: &ty_python_ast::StmtImportFrom,
		member: &str,
		export: Option<Module<'db>>,
	) -> ModuleResolution<'db> {
		self.revision.unwrap().value(self);
		let Some(referrer) = self.entry(importing_file.file(self)) else {
			return ModuleResolution::Fallback;
		};
		let request = resolve::Request::Import {
			imports: referrer.imports.clone(),
			level: import.level,
			name: import.module.as_deref().unwrap_or_default().to_owned(),
			referrer: referrer.module.clone(),
		};
		let resolve::Output::Resolved(resolution) =
			self.resolve(importing_file.file(self), request)
		else {
			return ModuleResolution::Fallback;
		};
		let Some(context) = resolution.context else {
			return ModuleResolution::Fallback;
		};
		let export = match export {
			None => resolve::Export::Absent,
			Some(module)
				if module
					.file(self)
					.and_then(|file| self.entry(file))
					.is_some() =>
			{
				resolve::Export::Module
			},
			Some(_) => resolve::Export::Value,
		};
		let request = resolve::Request::Member {
			context,
			export,
			name: member.to_owned(),
		};
		self.result(
			self.resolve(importing_file.file(self), request),
			importing_file.resolver_file(self).environment(self),
		)
	}

	fn resolve_module<'db>(
		&'db self,
		importing_file: ImportingFile<'db>,
		name: Option<&ModuleName>,
		level: u32,
	) -> ModuleResolution<'db> {
		self.revision.unwrap().value(self);
		let Some(referrer) = self.entry(importing_file.file(self)) else {
			return ModuleResolution::Fallback;
		};
		let environment = importing_file.resolver_file(self).environment(self);
		let request = resolve::Request::Import {
			imports: referrer.imports.clone(),
			level,
			name: name.map_or_else(String::new, |name| name.as_str().to_owned()),
			referrer: referrer.module.clone(),
		};
		self.result(
			self.resolve(importing_file.file(self), request),
			environment,
		)
	}

	fn resolve_submodule<'db>(
		&'db self,
		importing_file: ImportingFile<'db>,
		parent: Module<'db>,
		name: &ModuleName,
	) -> ModuleResolution<'db> {
		self.revision.unwrap().value(self);
		let target = if let Some(entry) = parent.file(self).and_then(|file| self.entry(file)) {
			resolve::Target::Module(resolve::Module::new(entry.module.clone()))
		} else {
			let modules = self.modules.lock().unwrap();
			let Some((_, namespace)) = modules
				.namespaces
				.values()
				.find(|(name, _)| name == parent.name(self))
			else {
				return ModuleResolution::Fallback;
			};
			resolve::Target::Namespace(namespace.clone())
		};
		let child = self
			.modules
			.lock()
			.unwrap()
			.children
			.get(&(target.key().to_owned(), name.as_str().to_owned()))
			.cloned();
		if let Some(child) = child {
			Self::target_keys(
				&child,
				self.modules
					.lock()
					.unwrap()
					.links
					.entry(importing_file.file(self))
					.or_default(),
			);
			let environment = parent.resolver_environment(self);
			return ModuleResolution::Resolved(self.target_module(&child, environment));
		}
		let Some(context) = target.context() else {
			return ModuleResolution::NotFound;
		};
		let request = resolve::Request::Child {
			context,
			name: name.as_str().to_owned(),
		};
		self.result(
			self.resolve(importing_file.file(self), request),
			parent.resolver_environment(self),
		)
	}

	fn file_to_module<'db>(&'db self, file: ResolverFile<'db>) -> ModuleResolution<'db> {
		self.revision.unwrap().value(self);
		match self.entry(file.file(self)) {
			None => ModuleResolution::Fallback,
			Some(entry) => ModuleResolution::Resolved(self.module(&entry, file.environment(self))),
		}
	}

	fn entry(&self, file: File) -> Option<Arc<Entry>> {
		let modules = self.modules.lock().unwrap();
		modules
			.files
			.get(&file)
			.map(|index| modules.entries[*index].clone())
	}

	#[must_use]
	fn module<'db>(&'db self, entry: &Entry, environment: ResolverEnvironment<'db>) -> Module<'db> {
		let kind = if entry.package {
			ModuleKind::Package
		} else {
			ModuleKind::Module
		};
		Module::new(self, entry.file, environment, entry.name.clone(), kind)
	}

	fn register_target(&self, target: &resolve::Target) -> tg::Result<()> {
		match target {
			resolve::Target::Module(module) => {
				self.register(module.data.clone())?;
			},
			resolve::Target::Namespace(namespace) => {
				self.register_target(&namespace.parent)?;
				let mut modules = self.modules.lock().unwrap();
				if let Some((_, previous)) = modules.namespaces.get_mut(&namespace.key) {
					// Match the runtime: retain directory lookup across later file-only resolutions.
					if namespace.referrer.kind == tg::module::Kind::Directory
						|| previous.referrer.kind != tg::module::Kind::Directory
					{
						*previous = namespace.clone();
					}
				} else {
					let name = ModuleName::new(&format!("n{}", modules.namespaces.len())).unwrap();
					modules
						.namespaces
						.insert(namespace.key.clone(), (name, namespace.clone()));
				}
			},
		}
		Ok(())
	}

	#[must_use]
	fn target_module<'db>(
		&'db self,
		target: &resolve::Target,
		environment: ResolverEnvironment<'db>,
	) -> Module<'db> {
		let modules = self.modules.lock().unwrap();
		match target {
			resolve::Target::Module(module) => {
				self.module(&modules.entries[modules.keys[&module.key]], environment)
			},
			resolve::Target::Namespace(namespace) => {
				let (name, _) = &modules.namespaces[&namespace.key];
				Module::namespace_package(self, environment, Cow::Owned(name.clone()))
			},
		}
	}

	fn result<'db>(
		&'db self,
		output: resolve::Output,
		environment: ResolverEnvironment<'db>,
	) -> ModuleResolution<'db> {
		let result = (|| -> tg::Result<resolve::Output> {
			let resolve::Output::Resolved(resolution) = output else {
				return Ok(output);
			};
			for step in &resolution.steps {
				self.register_target(&step.parent)?;
				self.register_target(&step.target)?;
				self.modules.lock().unwrap().children.insert(
					(step.parent.key().to_owned(), step.name.clone()),
					step.target.clone(),
				);
			}
			self.register_target(&resolution.target)?;
			Ok(resolve::Output::Resolved(resolution))
		})();
		match result {
			Ok(resolve::Output::Fallback) => ModuleResolution::Fallback,
			Ok(resolve::Output::Missing { .. }) => ModuleResolution::NotFound,
			Ok(resolve::Output::Resolved(resolution)) => {
				ModuleResolution::Resolved(self.target_module(&resolution.target, environment))
			},
			Err(error) => {
				self.error.lock().unwrap().get_or_insert(error);
				ModuleResolution::NotFound
			},
		}
	}

	fn dependencies(&self, file: File) -> Vec<Arc<Entry>> {
		let mut keys = BTreeSet::new();
		if let Some(records) = self.resolutions.lock().unwrap().get(&file) {
			for record in records.values() {
				if let resolve::Output::Resolved(resolution) = &record.output {
					Self::target_keys(&resolution.target, &mut keys);
					if let Some(root) = &resolution.root {
						Self::target_keys(root, &mut keys);
					}
					for step in &resolution.steps {
						Self::target_keys(&step.parent, &mut keys);
						Self::target_keys(&step.target, &mut keys);
					}
				}
			}
		}
		let modules = self.modules.lock().unwrap();
		if let Some(links) = modules.links.get(&file) {
			keys.extend(links.iter().cloned());
		}
		keys.into_iter()
			.filter_map(|key| {
				modules
					.keys
					.get(&key)
					.map(|index| modules.entries[*index].clone())
			})
			.collect()
	}

	fn target_keys(target: &resolve::Target, keys: &mut BTreeSet<String>) {
		match target {
			resolve::Target::Module(module) => {
				keys.insert(module.key.clone());
			},
			resolve::Target::Namespace(namespace) => Self::target_keys(&namespace.parent, keys),
		}
	}

	fn resolve(&self, file: File, request: resolve::Request) -> resolve::Output {
		let key = serde_json::to_string(&request).unwrap();
		let output = self.resolve_inner(request.clone());
		let resolution = Resolution {
			output: output.clone(),
			request,
		};
		self.resolutions
			.lock()
			.unwrap()
			.entry(file)
			.or_default()
			.insert(key, resolution);
		output
	}

	fn resolve_inner(&self, request: resolve::Request) -> resolve::Output {
		let paths = self
			.documents
			.values()
			.map(|document| &document.module)
			.filter_map(|module| match &module.referent.node {
				tg::module::data::Source::Path(path) if module.kind == tg::module::Kind::Python => {
					Some(path.clone())
				},
				_ => None,
			})
			.collect();
		let resolver = resolve::Resolver::with_paths(self.compiler.instance.clone(), paths);
		match self
			.compiler
			.main_runtime_handle
			.block_on(resolver.resolve(request))
		{
			Ok(output) => output,
			Err(error) => {
				// Match JavaScript: let the checker report unresolved imports at their source locations.
				tracing::debug!(error = %error.trace(), "Python module resolution failed");
				resolve::Output::Missing {
					message: "cannot find the module".to_owned(),
					name: None,
				}
			},
		}
	}

	fn diagnostic(
		&self,
		diagnostic: &Diagnostic,
		encoding: tg::position::Encoding,
	) -> tg::Result<tg::Diagnostic> {
		let location = diagnostic
			.primary_span()
			.map(|span| -> tg::Result<_> {
				let UnifiedFile::Ty(file) = span.file() else {
					return Ok(None);
				};
				let Some(entry) = self.entry(*file) else {
					return Ok(None);
				};
				let bytes = span.range().map_or(0..0, |range| {
					usize::from(range.start())..usize::from(range.end())
				});
				let range = tg::Range::try_from_byte_range_in_string(&entry.text, bytes, encoding)
					.ok_or_else(|| tg::error!("invalid python diagnostic source range"))?;
				Ok(Some(tg::module::data::Location {
					module: entry.module.without_token(),
					range,
				}))
			})
			.transpose()?
			.flatten();
		let severity = match diagnostic.severity() {
			Severity::Fatal | Severity::Error => tg::diagnostic::Severity::Error,
			Severity::Warning => tg::diagnostic::Severity::Warning,
			Severity::Info => tg::diagnostic::Severity::Info,
		};
		let mut message = diagnostic.concise_message().to_string();
		let mut seen = BTreeSet::new();
		for annotation in diagnostic
			.annotations()
			.iter()
			.filter(|annotation| !annotation.is_primary())
		{
			if let Some(note) = annotation
				.get_message()
				.filter(|message| !message.is_empty())
				&& seen.insert(note.to_owned())
			{
				write!(message, "\n{note}").unwrap();
			}
		}
		for sub in diagnostic.sub_diagnostics() {
			let note = sub.concise_message().to_string();
			if !note.is_empty() && seen.insert(note.clone()) {
				write!(message, "\n{}: {note}", sub.severity()).unwrap();
			}
			for annotation in sub
				.annotations()
				.iter()
				.filter(|annotation| !annotation.is_primary())
			{
				if let Some(note) = annotation
					.get_message()
					.filter(|message| !message.is_empty())
					&& seen.insert(note.to_owned())
				{
					write!(message, "\n{note}").unwrap();
				}
			}
		}
		let diagnostic = tg::diagnostic::Data {
			location,
			message,
			severity,
		};
		diagnostic.try_into()
	}
}

#[salsa::db]
impl salsa::Database for Database {}

#[salsa::db]
impl ruff_db::Db for Database {
	fn vendored(&self) -> &VendoredFileSystem {
		ty_vendored::file_system()
	}
	fn system(&self) -> &dyn ruff_db::system::System {
		&self.system
	}
	fn files(&self) -> &Files {
		&self.files
	}
}

#[salsa::db]
impl ty_module_resolver::Db for Database {
	fn module_display_name<'db>(&'db self, module: Module<'db>) -> Option<Cow<'db, str>> {
		Self::module_display_name(self, module)
	}

	fn resolve_import_member<'db>(
		&'db self,
		importing_file: ImportingFile<'db>,
		_parent: Module<'db>,
		import: &ty_python_ast::StmtImportFrom,
		member: &str,
		export: Option<Module<'db>>,
		_mode: ModuleResolveMode,
	) -> ModuleResolution<'db> {
		Self::resolve_import_member(self, importing_file, import, member, export)
	}

	fn resolve_module<'db>(
		&'db self,
		importing_file: ImportingFile<'db>,
		name: Option<&ModuleName>,
		level: u32,
		_mode: ModuleResolveMode,
	) -> ModuleResolution<'db> {
		Self::resolve_module(self, importing_file, name, level)
	}

	fn resolve_submodule<'db>(
		&'db self,
		importing_file: ImportingFile<'db>,
		parent: Module<'db>,
		name: &ModuleName,
		_mode: ModuleResolveMode,
	) -> ModuleResolution<'db> {
		Self::resolve_submodule(self, importing_file, parent, name)
	}

	fn file_to_module<'db>(&'db self, file: ResolverFile<'db>) -> ModuleResolution<'db> {
		Self::file_to_module(self, file)
	}
}

#[salsa::db]
impl ty_python_core::Db for Database {
	fn should_check_file(&self, file: File) -> bool {
		self.entry(file)
			.is_some_and(|entry| entry.module.kind == tg::module::Kind::Python)
	}
}

#[salsa::db]
impl ty_python_semantic::Db for Database {
	fn check_file(&self, file: File) -> Vec<Diagnostic> {
		ty_python_semantic::check_file_unwrap(self, self.program_file(file))
	}
	fn program_file(&self, file: File) -> ProgramFile<'_> {
		Program::from_settings(self, &self.settings).program_file(self, file)
	}
	fn python_version_with_source(&self, _file: File) -> &PythonVersionWithSource {
		&self.settings.python_version
	}
	fn rule_selection(&self, _file: File) -> &RuleSelection {
		&self.rules
	}
	fn lint_registry(&self) -> &LintRegistry {
		ty_python_semantic::default_lint_registry()
	}
	fn analysis_settings(&self, _file: File) -> &AnalysisSettings {
		&self.analysis
	}
	fn dependency_metadata(&self, _file: File) -> Option<&DependencyMetadata> {
		None
	}
	fn verbose(&self) -> bool {
		false
	}
	fn is_open_file(&self, file: File) -> bool {
		self.revision.unwrap().value(self);
		self.entry(file)
			.is_some_and(|entry| self.documents.contains_key(&Key::new(&entry.module)))
	}
	fn dyn_clone(&self) -> Box<dyn ty_python_semantic::Db> {
		Box::new(self.clone())
	}
}

#[salsa::db]
impl ty_project::Db for Database {
	fn project(&self) -> ty_project::Project {
		self.project.unwrap()
	}

	fn uv_environments(&self) -> &ty_project::UvEnvironments {
		&self.uv_environments
	}

	fn dyn_clone(&self) -> Box<dyn ty_project::Db> {
		Box::new(self.clone())
	}
}
