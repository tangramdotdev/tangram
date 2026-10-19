use {
	super::{host::Host, resolver::Factory},
	crate::Compiler,
	std::{
		cell::RefCell,
		collections::BTreeSet,
		sync::{Arc, RwLock},
	},
	tangram_client::prelude::*,
	tsrs_core::{
		CompilerOptions, ModuleDetectionKind, ModuleKind, P, ScriptTarget, Tristate,
		context::Context,
	},
	tsrs_lsproto as protocol,
	tsrs_project::{
		APICreateProgramRequest, APIReconfigureProgramRequest, APISnapshotRequest,
		FileChangeSummary, SessionInit, SessionOptions, Snapshot, SnapshotHost, SyntheticProjectID,
		new_snapshot_host,
	},
};

pub(super) struct Project {
	pub(super) host: Arc<Host>,
	pub(super) id: SyntheticProjectID,
	language_service: RefCell<Option<tsrs_ls::LanguageService>>,
	language_service_host: Arc<LanguageServiceHost>,
	prepared_files: RefCell<BTreeSet<String>>,
	resolver_id: u64,
	root_file_names: Vec<String>,
	pub(super) snapshot: RefCell<SnapshotLease>,
	snapshot_host: Arc<SnapshotHost>,
}

struct LanguageServiceHost {
	snapshot: RwLock<Arc<Snapshot>>,
}

pub(super) struct SnapshotLease(pub(super) Arc<Snapshot>);

impl Project {
	pub(super) fn new(compiler: &Compiler, modules: Vec<tg::module::Data>) -> tg::Result<Self> {
		// Create the filesystem and the native snapshot host.
		let host = Host::new(compiler.clone());
		let host = Arc::new(host);
		let context = Self::context();
		let options = SessionOptions {
			current_directory: "/".into(),
			default_library_path: "/__library__".into(),
			position_encoding: match *compiler.position_encoding.read().unwrap() {
				tg::position::Encoding::Utf8 => protocol::PositionEncodingKind::UTF8,
				tg::position::Encoding::Utf16 => protocol::PositionEncodingKind::UTF16,
			},
			..Default::default()
		};
		let init = SessionInit {
			background_ctx: context.clone(),
			client: None,
			content_mapped_parse_cache: None,
			fs: host.clone(),
			logger: None,
			npm_executor: None,
			options: Arc::new(options),
			parse_cache: None,
		};
		let snapshot_host = new_snapshot_host(&init);
		let root = SnapshotLease(snapshot_host.new_root_snapshot());

		// Create a native program with Tangram's module resolver.
		let mut root_file_names = modules
			.into_iter()
			.map(|module| host.path(&module))
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect::<Vec<_>>();
		root_file_names.push("/__library__/tangram.d.ts".into());
		let compiler_options = CompilerOptions {
			allow_js: Tristate::True,
			check_js: Tristate::True,
			exact_optional_property_types: Tristate::True,
			isolated_modules: Tristate::True,
			lib: Some(Vec::new()),
			module: ModuleKind::ESNext,
			module_detection: ModuleDetectionKind::Force,
			no_emit: Tristate::True,
			no_unchecked_indexed_access: Tristate::True,
			no_unchecked_side_effect_imports: Tristate::True,
			skip_lib_check: Tristate::True,
			strict: Tristate::True,
			target: ScriptTarget::ESNext,
			types: Some(Vec::new()),
			verbatim_module_syntax: Tristate::True,
			..Default::default()
		};
		let roots = root_file_names.clone();
		let program = APICreateProgramRequest {
			compiler_options: Some(P::new(compiler_options)),
			config_file_parsing_diagnostics: Vec::new(),
			module_resolver_factory: Some(Arc::new(Factory(host.clone()))),
			module_resolver_id: 1,
			project_references: Vec::new(),
			root_file_names,
		};
		let request = APISnapshotRequest {
			create_programs: vec![program],
			ensure_all_programs: true,
			..Default::default()
		};
		let snapshot = match snapshot_host.clone_snapshot(
			&context,
			&root.0,
			FileChangeSummary::default(),
			Some(Arc::new(request)),
		) {
			Ok(snapshot) => SnapshotLease(snapshot),
			Err((snapshot, error)) => {
				let _snapshot = SnapshotLease(snapshot);
				return Err(tg::error!(!error, "failed to create the tsrs program"));
			},
		};
		let id = snapshot
			.0
			.created_programs()
			.first()
			.and_then(Option::as_ref)
			.and_then(|project| project.id().synthetic())
			.ok_or_else(|| tg::error!("the tsrs snapshot did not contain a synthetic project"))?;
		let language_service_host = LanguageServiceHost {
			snapshot: RwLock::new(snapshot.0.clone()),
		};
		let project = Self {
			host,
			id,
			language_service: RefCell::new(None),
			language_service_host: Arc::new(language_service_host),
			prepared_files: RefCell::default(),
			resolver_id: 1,
			root_file_names: roots,
			snapshot: RefCell::new(snapshot),
			snapshot_host,
		};
		project
			.host
			.prune(project.program()?, &project.root_file_names);

		Ok(project)
	}

	pub(super) fn update(&mut self, modules: Vec<tg::module::Data>) -> tg::Result<()> {
		// Synchronize the roots and source revisions with the compiler's document store.
		let roots = modules;
		let mut root_file_names = roots
			.iter()
			.map(|module| self.host.path(module))
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect::<Vec<_>>();
		root_file_names.push("/__library__/tangram.d.ts".into());
		let mut changes = self.host.changes()?;
		let roots_changed = root_file_names != self.root_file_names;
		if !roots_changed && changes.files.is_empty() && !changes.resolutions {
			return Ok(());
		}

		// Update the existing synthetic project and retain the native parse cache.
		let context = Self::context();
		let mut request = APISnapshotRequest {
			ensure_all_programs: true,
			..Default::default()
		};
		let resolver_id = self.resolver_id + u64::from(changes.resolutions);
		if roots_changed || changes.resolutions {
			let program = APICreateProgramRequest {
				compiler_options: None,
				config_file_parsing_diagnostics: Vec::new(),
				module_resolver_factory: Some(Arc::new(Factory(self.host.clone()))),
				module_resolver_id: resolver_id,
				project_references: Vec::new(),
				root_file_names: root_file_names.clone(),
			};
			let program = APIReconfigureProgramRequest {
				program_id: self.id.clone(),
				request: program,
			};
			request.reconfigure_programs.push(program);
		}
		let snapshot = self
			.snapshot_host
			.clone_snapshot(
				&context,
				&self.snapshot.borrow().0,
				std::mem::take(&mut changes.files),
				Some(Arc::new(request)),
			)
			.map_err(|(snapshot, error)| {
				let _snapshot = SnapshotLease(snapshot);
				self.host.restore(changes);
				tg::error!(!error, "failed to update the tsrs project")
			})?;
		self.set_snapshot(SnapshotLease(snapshot))?;
		self.prepared_files.borrow_mut().clear();
		self.resolver_id = resolver_id;
		self.root_file_names = root_file_names;
		self.host.prune(self.program()?, &self.root_file_names);

		Ok(())
	}

	fn set_snapshot(&self, snapshot: SnapshotLease) -> tg::Result<()> {
		let program = snapshot
			.0
			.project_collection
			.get_project(&self.id.as_id())
			.and_then(|project| project.get_program())
			.ok_or_else(|| tg::error!("the updated tsrs snapshot did not contain the project"))?;
		if !std::ptr::eq(program, self.program()?) {
			self.language_service.borrow_mut().take();
			self.prepared_files.borrow_mut().clear();
		}
		*self.language_service_host.snapshot.write().unwrap() = snapshot.0.clone();
		self.snapshot.replace(snapshot);
		Ok(())
	}

	pub(super) fn program(&self) -> tg::Result<&'static tsrs_compiler::Program> {
		self.snapshot
			.borrow()
			.0
			.project_collection
			.get_project(&self.id.as_id())
			.and_then(|project| project.get_program())
			.ok_or_else(|| tg::error!("the tsrs project did not contain a program"))
	}

	pub(super) fn with_language_service<T>(
		&self,
		module: &tg::module::Data,
		f: impl FnOnce(&Context, &tsrs_ls::LanguageService) -> tg::Result<T>,
	) -> tg::Result<T> {
		self.with_language_service_inner(module, false, f)
	}

	pub(super) fn with_language_service_with_auto_imports<T>(
		&self,
		module: &tg::module::Data,
		f: impl FnOnce(&Context, &tsrs_ls::LanguageService) -> tg::Result<T>,
	) -> tg::Result<T> {
		self.with_language_service_inner(module, true, f)
	}

	fn with_language_service_inner<T>(
		&self,
		module: &tg::module::Data,
		auto_imports: bool,
		f: impl FnOnce(&Context, &tsrs_ls::LanguageService) -> tg::Result<T>,
	) -> tg::Result<T> {
		// Prepare each file once for the current project version.
		let path = self.host.path(module);
		let context = Self::context();
		if auto_imports && !self.prepared_files.borrow().contains(&path) {
			let uri = self.uri(module);
			let snapshot = self.snapshot_host.clone_snapshot_with_auto_imports(
				&context,
				&self.snapshot.borrow().0,
				&uri,
				None,
			);
			self.set_snapshot(SnapshotLease(snapshot))?;
			self.prepared_files.borrow_mut().insert(path.clone());
		}
		let program = self.program()?;
		if program.get_source_file(&path).is_none() {
			return Err(tg::error!(
				"the tsrs program did not contain the requested module"
			));
		}

		// Reuse the native language service while its compiler program is unchanged.
		let mut service = self.language_service.borrow_mut();
		let service = service.get_or_insert_with(|| {
			let host: Arc<dyn tsrs_ls::Host> = self.language_service_host.clone();
			tsrs_ls::new_language_service(
				tsrs_ls::autoimport::ProjectID(self.id.0.clone()),
				program,
				host,
				&path,
			)
		});
		let response = f(&context, service)?;

		Ok(response)
	}

	pub(super) fn context() -> Context {
		let mut capabilities = protocol::ResolvedClientCapabilities::default();
		capabilities
			.text_document
			.document_symbol
			.hierarchical_document_symbol_support = true;
		capabilities.text_document.semantic_tokens.token_types = crate::semantic_tokens::TYPES
			.iter()
			.map(|value| (*value).to_owned())
			.collect();
		capabilities.text_document.semantic_tokens.token_modifiers =
			crate::semantic_tokens::MODIFIERS
				.iter()
				.map(|value| (*value).to_owned())
				.collect();
		protocol::with_client_capabilities(&Context::background(), Arc::new(capabilities))
	}
}

impl tsrs_ls::Host for LanguageServiceHost {
	fn use_case_sensitive_file_names(&self) -> bool {
		self.snapshot
			.read()
			.unwrap()
			.use_case_sensitive_file_names()
	}

	fn read_file(&self, path: &str) -> Option<String> {
		self.snapshot.read().unwrap().read_file(path)
	}

	fn converters(&self) -> Arc<tsrs_ls::lsconv::Converters> {
		self.snapshot.read().unwrap().converters()
	}

	fn get_preferences(&self, active_file: &str) -> tsrs_ls::lsutil::UserPreferences {
		let mut preferences =
			tsrs_ls::Host::get_preferences(self.snapshot.read().unwrap().as_ref(), active_file);
		preferences.auto_import_specifier_exclude_regexes = Some(vec![".*".into()]);
		preferences.include_completions_for_import_statements = Tristate::False;
		preferences.include_completions_for_module_exports = Tristate::False;
		preferences
	}

	fn get_ecma_line_info(&self, path: &str) -> Option<Arc<tsrs_ls::sourcemap::ECMALineInfo>> {
		self.snapshot.read().unwrap().get_ecma_line_info(path)
	}

	fn auto_import_registry(&self) -> Option<Arc<tsrs_ls::autoimport::Registry>> {
		self.snapshot.read().unwrap().auto_import_registry()
	}

	fn read_directory(
		&self,
		current_directory: &str,
		path: &str,
		extensions: &[String],
		excludes: &[String],
		includes: &[String],
		depth: usize,
	) -> Vec<String> {
		tsrs_ls::Host::read_directory(
			self.snapshot.read().unwrap().as_ref(),
			current_directory,
			path,
			extensions,
			excludes,
			includes,
			depth,
		)
	}

	fn get_directories(&self, path: &str) -> Vec<String> {
		tsrs_ls::Host::get_directories(self.snapshot.read().unwrap().as_ref(), path)
	}

	fn directory_exists(&self, path: &str) -> bool {
		tsrs_ls::Host::directory_exists(self.snapshot.read().unwrap().as_ref(), path)
	}

	fn file_exists(&self, path: &str) -> bool {
		tsrs_ls::Host::file_exists(self.snapshot.read().unwrap().as_ref(), path)
	}
}

impl Drop for SnapshotLease {
	fn drop(&mut self) {
		self.0.deref();
	}
}
