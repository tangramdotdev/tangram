use {
	super::{library, resolve, system::System},
	crate::{Compiler, analyze::py::metadata},
	ruff_db::{
		diagnostic::{Diagnostic, Severity, UnifiedFile},
		files::{File, Files, system_path_to_file},
		system::SystemPathBuf,
		vendored::VendoredFileSystem,
	},
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

#[salsa::db]
#[derive(Clone)]
// Each check owns an append-only source snapshot; loaded texts and resolution inputs never change.
pub(super) struct Database {
	analysis: Arc<AnalysisSettings>,
	compiler: Compiler,
	error: Arc<Mutex<Option<tg::Error>>>,
	files: Files,
	modules: Arc<Mutex<Modules>>,
	rules: Arc<RuleSelection>,
	settings: Arc<ProgramSettings>,
	storage: salsa::Storage<Self>,
	system: System,
}

#[derive(Default)]
struct Modules {
	children: BTreeMap<(String, String), resolve::Target>,
	entries: Vec<Arc<Entry>>,
	files: HashMap<File, usize>,
	keys: BTreeMap<String, usize>,
	namespaces: BTreeMap<String, (ModuleName, Box<resolve::Namespace>)>,
}

struct Entry {
	diagnostics: Vec<tg::Diagnostic>,
	file: File,
	imports: BTreeMap<String, tg::module::Import>,
	module: tg::module::Data,
	name: ModuleName,
	package: bool,
	text: String,
}

impl Database {
	pub(super) fn new(compiler: Compiler) -> tg::Result<Self> {
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
			.map_err(|error| tg::error!(!error, "failed to configure the Python library"))?;
		Ok(Self {
			analysis: Arc::new(AnalysisSettings::default()),
			compiler,
			error: Arc::default(),
			files: Files::default(),
			modules: Arc::default(),
			rules: Arc::new(RuleSelection::from_registry(
				ty_python_semantic::default_lint_registry(),
			)),
			settings: Arc::new(settings),
			storage: salsa::Storage::default(),
			system,
		})
	}

	pub(super) fn check(&self, modules: Vec<tg::module::Data>) -> tg::Result<Vec<tg::Diagnostic>> {
		for module in modules {
			self.register(module)?;
		}
		let mut diagnostics = Vec::new();
		let mut index = 0;
		// Resolution can discover additional files; every canonical module is checked once.
		loop {
			let entry = self.modules.lock().unwrap().entries.get(index).cloned();
			let Some(entry) = entry else {
				break;
			};
			index += 1;
			diagnostics.extend(entry.diagnostics.iter().cloned());
			if entry.module.kind != tg::module::Kind::Py {
				continue;
			}
			let program = Program::from_settings(self, &self.settings);
			let file = program.program_file(self, entry.file);
			let results = ty_python_semantic::check_file(self, file)
				.unwrap_or_else(|error| vec![error].into_boxed_slice());
			for diagnostic in &results {
				diagnostics.push(self.diagnostic(diagnostic)?);
			}
		}
		if let Some(error) = self.error.lock().unwrap().take() {
			return Err(error);
		}
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
				return Ok(modules.entries[*index].clone());
			}
		}
		let text = self
			.compiler
			.main_runtime_handle
			.block_on(
				self.compiler
					.load_module_with_language(&module, Some(tg::module::load::Language::Py)),
			)
			.map_err(|error| tg::error!(!error, %module, "failed to load the Python module"))?;
		let descriptor = resolve::Module::new(module.clone());
		let path = descriptor.filename.as_path();
		let package = descriptor.package;
		let mut diagnostics = Vec::new();
		// Syntax diagnostics belong to ty; parse metadata only when the module is syntactically valid.
		let imports = if ruff_python_parser::parse_module(&text).is_ok() {
			match metadata::parse(path, &text) {
				Ok(metadata) => metadata.imports,
				Err(error) => {
					diagnostics.push(Self::metadata_diagnostic(&module, error)?);
					BTreeMap::new()
				},
			}
		} else {
			BTreeMap::new()
		};
		let mut modules = self.modules.lock().unwrap();
		if let Some(index) = modules.keys.get(&key) {
			return Ok(modules.entries[*index].clone());
		}
		let index = modules.entries.len();
		let name = ModuleName::new(&format!("m{index}")).unwrap();
		let filename = if package { "__init__.py" } else { "module.py" };
		let path = SystemPathBuf::from(format!("/modules/{index}/{filename}"));
		self.system
			.memory
			.create_directory_all(path.parent().unwrap())
			.map_err(|error| tg::error!(!error, "failed to create the Python module directory"))?;
		self.system
			.memory
			.write_file(&path, &text)
			.map_err(|error| tg::error!(!error, "failed to load the Python module source"))?;
		let file = system_path_to_file(self, &path)
			.map_err(|error| tg::error!(!error, "failed to create the Python source file"))?;
		let entry = Arc::new(Entry {
			diagnostics,
			file,
			imports,
			module,
			name,
			package,
			text,
		});
		modules.files.insert(file, index);
		modules.keys.insert(key, index);
		modules.entries.push(entry.clone());
		drop(modules);
		// The runtime loads containing package initializers before executing a module.
		let request = resolve::Request::Package {
			module: entry.module.clone(),
			parent: entry.package,
		};
		if entry.module.kind == tg::module::Kind::Py
			&& let resolve::Output::Resolved(resolution) = self.resolve(request)
		{
			self.register_target(&resolution.target)?;
		}
		Ok(entry)
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

	fn entry(&self, file: File) -> Option<Arc<Entry>> {
		let modules = self.modules.lock().unwrap();
		modules
			.files
			.get(&file)
			.map(|index| modules.entries[*index].clone())
	}

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

	fn resolve(&self, request: resolve::Request) -> resolve::Output {
		let resolver = resolve::Resolver::new(self.compiler.instance.clone());
		match self
			.compiler
			.main_runtime_handle
			.block_on(resolver.resolve(request))
		{
			Ok(output) => output,
			Err(error) => {
				// Match JS: let the checker report unresolved imports at their source locations.
				tracing::debug!(error = %error, "Python module resolution failed");
				resolve::Output::Missing {
					message: "cannot find the module".to_owned(),
					name: None,
				}
			},
		}
	}

	fn diagnostic(&self, diagnostic: &Diagnostic) -> tg::Result<tg::Diagnostic> {
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
				let range = tg::Range::try_from_byte_range_in_string(
					&entry.text,
					bytes,
					tg::position::Encoding::Utf8,
				)
				.ok_or_else(|| tg::error!("invalid Python diagnostic source range"))?;
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
		_parent: Module<'db>,
		import: &ty_python_ast::StmtImportFrom,
		member: &str,
		export: Option<Module<'db>>,
		_mode: ModuleResolveMode,
	) -> ModuleResolution<'db> {
		let Some(referrer) = self.entry(importing_file.file(self)) else {
			return ModuleResolution::Fallback;
		};
		let request = resolve::Request::Import {
			imports: referrer.imports.clone(),
			level: import.level,
			name: import.module.as_deref().unwrap_or_default().to_owned(),
			referrer: referrer.module.clone(),
		};
		let resolve::Output::Resolved(resolution) = self.resolve(request) else {
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
			self.resolve(request),
			importing_file.resolver_file(self).environment(self),
		)
	}

	fn resolve_module<'db>(
		&'db self,
		importing_file: ImportingFile<'db>,
		name: Option<&ModuleName>,
		level: u32,
		_mode: ModuleResolveMode,
	) -> ModuleResolution<'db> {
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
		self.result(self.resolve(request), environment)
	}

	fn resolve_submodule<'db>(
		&'db self,
		_importing_file: ImportingFile<'db>,
		parent: Module<'db>,
		name: &ModuleName,
		_mode: ModuleResolveMode,
	) -> ModuleResolution<'db> {
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
		self.result(self.resolve(request), parent.resolver_environment(self))
	}

	fn file_to_module<'db>(&'db self, file: ResolverFile<'db>) -> ModuleResolution<'db> {
		match self.entry(file.file(self)) {
			Some(entry) => ModuleResolution::Resolved(self.module(&entry, file.environment(self))),
			None => ModuleResolution::Fallback,
		}
	}
}

#[salsa::db]
impl ty_python_core::Db for Database {
	fn should_check_file(&self, file: File) -> bool {
		self.entry(file)
			.is_some_and(|entry| entry.module.kind == tg::module::Kind::Py)
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
	fn is_open_file(&self, _file: File) -> bool {
		false
	}
	fn dyn_clone(&self) -> Box<dyn ty_python_semantic::Db> {
		Box::new(self.clone())
	}
}
