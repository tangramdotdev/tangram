use {
	crate::{
		Compiler, check, code_action, completion, diagnostics, document_link, folding_range, hover,
		inlay_hint, semantic_tokens, symbols,
	},
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
};

/// The backend for config modules, whose runtimes run them, and for interface modules, which declare the interfaces of config modules.
pub(crate) struct Backend;

/// Get the module of a config's interface: the interface beside the config, which the config imports, or else the interface for configs in its runtime package.
pub async fn try_get_interface_module<I>(
	instance: &I,
	module: &tg::module::Data,
	text: &str,
) -> tg::Result<Option<tg::module::Data>>
where
	I: tg::Instance,
{
	if let Some(module) = try_get_sibling_interface_module(instance, module).await? {
		return Ok(Some(module));
	}
	let module = try_get_runtime_interface_module(instance, module, text).await?;
	Ok(module)
}

impl crate::backend::Backend for Backend {
	fn feature(&self) -> &'static str {
		"config"
	}

	fn code_action(
		&self,
		_compiler: &Compiler,
		_arg: code_action::Arg,
	) -> BoxFuture<'static, tg::Result<code_action::Output>> {
		futures::future::ok(code_action::Output { actions: None }).boxed()
	}

	fn completion(
		&self,
		_compiler: &Compiler,
		_arg: completion::Arg,
	) -> BoxFuture<'static, tg::Result<completion::Output>> {
		futures::future::ok(completion::Output { entries: None }).boxed()
	}

	fn document_diagnostics(
		&self,
		compiler: &Compiler,
		arg: diagnostics::DocumentArg,
	) -> BoxFuture<'static, tg::Result<diagnostics::DocumentOutput>> {
		let compiler = compiler.clone();
		async move {
			// Report an invalid config at the range of its error.
			let mut diagnostics = Vec::new();
			for module in arg.modules {
				let text = compiler.load_module(&module).await?;
				let path = match &module.referent.node {
					tg::module::data::Source::Edge(_) => module.referent.path(),
					tg::module::data::Source::Path(path) => Some(path.as_path()),
				};
				let Some(format) = path.and_then(tg::module::config::Format::for_path) else {
					continue;
				};
				let Err((error, range)) = tg::module::Config::parse_with_range(&text, format)
				else {
					continue;
				};
				let location = range.map(|range| tg::module::data::Location {
					module: module.without_token(),
					range,
				});
				let message = crate::interface::non_value_message(&error);
				let diagnostic = tg::diagnostic::Data {
					location,
					message,
					severity: tg::diagnostic::Severity::Error,
				};
				diagnostics.push(diagnostic);
			}

			let output = diagnostics::DocumentOutput { diagnostics };
			Ok(output)
		}
		.boxed()
	}

	fn document_link(
		&self,
		_compiler: &Compiler,
		_arg: document_link::Arg,
	) -> BoxFuture<'static, tg::Result<document_link::Output>> {
		futures::future::ok(document_link::Output { links: None }).boxed()
	}

	fn folding_range(
		&self,
		_compiler: &Compiler,
		_arg: folding_range::Arg,
	) -> BoxFuture<'static, tg::Result<folding_range::Output>> {
		futures::future::ok(folding_range::Output { ranges: None }).boxed()
	}

	fn hover(
		&self,
		_compiler: &Compiler,
		_arg: hover::Arg,
	) -> BoxFuture<'static, tg::Result<hover::Output>> {
		futures::future::ok(hover::Output { text: None }).boxed()
	}

	fn inlay_hint(
		&self,
		_compiler: &Compiler,
		_arg: inlay_hint::Arg,
	) -> BoxFuture<'static, tg::Result<inlay_hint::Output>> {
		futures::future::ok(inlay_hint::Output { hints: None }).boxed()
	}

	fn semantic_tokens(
		&self,
		_compiler: &Compiler,
		_arg: semantic_tokens::Arg,
	) -> BoxFuture<'static, tg::Result<semantic_tokens::Output>> {
		futures::future::ok(semantic_tokens::Output { tokens: None }).boxed()
	}

	fn symbols(
		&self,
		_compiler: &Compiler,
		_arg: symbols::Arg,
	) -> BoxFuture<'static, tg::Result<symbols::Output>> {
		futures::future::ok(symbols::Output { symbols: None }).boxed()
	}

	fn check(
		&self,
		compiler: &Compiler,
		arg: check::Arg,
	) -> BoxFuture<'static, tg::Result<check::Output>> {
		let compiler = compiler.clone();
		async move {
			// Validate each config and its interface, and each interface.
			for module in &arg.modules {
				let text = compiler.load_module(module).await?;
				Compiler::analyze(module, &text)?;
				if module.kind.is_config() {
					compiler.config_interface(module).await?;
				} else {
					compiler.read_interface(module).await?;
				}
			}

			let output = check::Output {
				diagnostics: Vec::new(),
			};
			Ok(output)
		}
		.boxed()
	}

	#[cfg(all(feature = "javascript", feature = "python"))]
	fn interface(
		&self,
		compiler: &Compiler,
		arg: crate::interfaces::Arg,
	) -> BoxFuture<'static, tg::Result<tg::Interface>> {
		let compiler = compiler.clone();
		async move {
			let interface = if arg.module.kind.is_config() {
				compiler.config_interface(&arg.module).await?
			} else {
				compiler.read_interface(&arg.module).await?
			};
			Ok(interface)
		}
		.boxed()
	}
}

impl Compiler {
	/// Get a config's interface from the interface beside it, or else from its runtime's interface for configs, or else an interface with no exports.
	pub(crate) async fn config_interface(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<tg::Interface> {
		let text = self.load_module(module).await?;
		let Some(module) = try_get_interface_module(&self.instance, module, &text).await? else {
			return Ok(tg::Interface::default());
		};
		let interface = self.read_interface(&module).await?;
		Ok(interface)
	}

	/// Get the interface modules that a config's interface may come from, including the interface files on disk that do not exist yet, so that creating one changes the config's interface.
	pub(crate) async fn config_interface_modules(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<Vec<tg::module::Data>> {
		let text = self.load_module(module).await?;
		let mut modules = Vec::new();

		// Add the interface beside the config.
		let path = match &module.referent.node {
			tg::module::data::Source::Path(path) => Some(path),
			tg::module::data::Source::Edge(_) => None,
		};
		if let Some(path) = path {
			if let Some(stem) = tg::module::config::interface_file_stem(path) {
				modules.extend(path_interface_modules(&path.with_file_name(stem)));
			}
		} else if let Some(module) =
			try_get_sibling_interface_module(&self.instance, module).await?
		{
			modules.push(module);
		}

		// Add the interface in the runtime package.
		let package = try_resolve_runtime_package(&self.instance, module, &text).await?;
		if let Some(package) = package
			&& let tg::module::data::Source::Path(path) = &package.referent.node
		{
			let path = path.join(tg::module::config::RUNTIME_INTERFACE_FILE_STEM);
			modules.extend(path_interface_modules(&path));
		} else if let Some(module) =
			try_get_runtime_interface_module(&self.instance, module, &text).await?
		{
			modules.push(module);
		}

		Ok(modules)
	}

	/// Read an interface module, and resolve its imports whose paths are relative to it.
	pub(crate) async fn read_interface(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<tg::Interface> {
		let text = self.load_module(module).await?;
		let language = tg::interface::Language::for_kind(module.kind)
			.ok_or_else(|| tg::error!("expected an interface module"))?;
		let mut interface = tg::interface::parse::parse(&text, language)?;
		for import in interface.imports.values_mut() {
			let tg::module::data::Source::Path(path) = &import.referent.node else {
				continue;
			};
			if path.is_absolute() {
				continue;
			}
			let path = path
				.to_str()
				.ok_or_else(|| tg::error!("invalid import path"))?;
			let reference = if path.starts_with('.') {
				path.parse()?
			} else {
				format!("./{path}").parse()?
			};
			let import_ = tg::module::Import {
				kind: Some(import.kind),
				reference,
			};
			let arg = tg::module::resolve::Arg {
				import: import_,
				referrer: Some(module.clone()),
			};
			let output = self.instance.resolve_module(arg).await.map_err(
				|error| tg::error!(!error, %path, "failed to resolve the import of the interface"),
			)?;
			*import = output.module;
		}
		Ok(interface)
	}
}

/// Get the module of the interface beside a config, which the config imports.
async fn try_get_sibling_interface_module<I>(
	instance: &I,
	module: &tg::module::Data,
) -> tg::Result<Option<tg::module::Data>>
where
	I: tg::Instance,
{
	// Get the stem of the file name of the interface.
	let path = match &module.referent.node {
		tg::module::data::Source::Edge(_) => module.referent.path(),
		tg::module::data::Source::Path(path) => Some(path.as_path()),
	};
	let Some(stem) = path.and_then(tg::module::config::interface_file_stem) else {
		return Ok(None);
	};

	// Get the dependencies of a checked-in config.
	let dependencies = match &module.referent.node {
		tg::module::data::Source::Path(_) => None,
		tg::module::data::Source::Edge(edge) => {
			let file = match tg::graph::Edge::<tg::Object>::try_from_data(edge.clone())? {
				tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
				tg::graph::Edge::Object(object) => object
					.try_unwrap_file()
					.map_err(|_| tg::error!("expected the config to be a file"))?,
				tg::graph::Edge::Pointer(pointer) => tg::File::with_pointer(pointer),
			};
			file.state()
				.set_location(module.referent.options.location.clone());
			file.state()
				.set_tokens(module.referent.options.tokens.clone());
			Some(file.dependencies_with_instance(instance).await?)
		},
	};

	// Find the interface that the config imports, in any language.
	let mut found = None;
	for language in tg::interface::Language::ALL {
		let name = format!("{stem}.{}", language.extension());
		let reference = format!("./{name}").parse::<tg::Reference>()?;
		let exists = match (&module.referent.node, &dependencies) {
			(tg::module::data::Source::Path(path), _) => {
				tokio::fs::try_exists(path.with_file_name(&name))
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to determine whether the interface exists")
					})?
			},
			(_, Some(dependencies)) => dependencies.contains_key(&reference),
			(_, None) => false,
		};
		if !exists {
			continue;
		}
		if found.is_some() {
			return Err(tg::error!(%stem, "the config has more than one interface beside it"));
		}
		found = Some((language, reference));
	}
	let Some((language, reference)) = found else {
		return Ok(None);
	};

	// Resolve the interface.
	let import = tg::module::Import {
		kind: Some(language.kind()),
		reference,
	};
	let arg = tg::module::resolve::Arg {
		import,
		referrer: Some(module.clone()),
	};
	let output = instance
		.resolve_module(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to resolve the interface of the config"))?;

	Ok(Some(output.module))
}

/// Get the module of the interface for configs in a config's runtime package, if the runtime is a package that has one.
async fn try_get_runtime_interface_module<I>(
	instance: &I,
	module: &tg::module::Data,
	text: &str,
) -> tg::Result<Option<tg::module::Data>>
where
	I: tg::Instance,
{
	// Resolve the runtime's package.
	let Some(package) = try_resolve_runtime_package(instance, module, text).await? else {
		return Ok(None);
	};
	let stem = tg::module::config::RUNTIME_INTERFACE_FILE_STEM;

	// Find the interface in the package, in any language.
	let mut found = None;
	for language in tg::interface::Language::ALL {
		let name = format!("{stem}.{}", language.extension());
		let referent = match &package.referent.node {
			tg::module::data::Source::Path(path) => {
				let path = path.join(&name);
				let exists = tokio::fs::try_exists(&path).await.map_err(|error| {
					tg::error!(!error, "failed to determine whether the interface exists")
				})?;
				if !exists {
					continue;
				}
				tg::Referent::with_node(tg::module::data::Source::Path(path))
			},
			tg::module::data::Source::Edge(edge) => {
				let directory = match tg::graph::Edge::<tg::Object>::try_from_data(edge.clone())? {
					tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
					tg::graph::Edge::Object(object) => object
						.try_unwrap_directory()
						.map_err(|_| tg::error!("expected the runtime to be a directory"))?,
					tg::graph::Edge::Pointer(pointer) => tg::Directory::with_pointer(pointer),
				};
				directory
					.state()
					.set_location(package.referent.options.location.clone());
				directory
					.state()
					.set_tokens(package.referent.options.tokens.clone());
				let Some(edge) = directory
					.try_get_entry_edge_with_instance(instance, &name)
					.await?
				else {
					continue;
				};
				let edge = tg::graph::Edge::<tg::Object>::from(edge).to_data();
				let path = package
					.referent
					.path()
					.map_or_else(|| name.clone().into(), |path| path.join(&name));
				let options = tg::referent::Options {
					path: Some(path),
					..package.referent.options.clone()
				};
				tg::Referent {
					node: tg::module::data::Source::Edge(edge),
					options,
				}
			},
		};
		if found.is_some() {
			return Err(
				tg::error!(%stem, "the runtime package has more than one interface for configs"),
			);
		}
		let module = tg::module::Data {
			kind: language.kind(),
			referent,
		};
		found = Some(module);
	}

	Ok(found)
}

/// Resolve a config's runtime as an artifact, returning it if it is a package directory and `None` if it is a single file.
async fn try_resolve_runtime_package<I>(
	instance: &I,
	module: &tg::module::Data,
	text: &str,
) -> tg::Result<Option<tg::module::Data>>
where
	I: tg::Instance,
{
	// Resolve the runtime.
	let format = tg::module::config::Format::for_kind(module.kind)
		.ok_or_else(|| tg::error!("expected a config module"))?;
	let config = tg::module::Config::parse(text, format)?;
	let mut import = config.runtime.import()?;
	import.kind = Some(tg::module::Kind::Artifact);
	let arg = tg::module::resolve::Arg {
		import,
		referrer: Some(module.clone()),
	};
	let output = instance
		.resolve_module(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to resolve the runtime of the config"))?;
	let package = output.module;

	// Determine whether the runtime is a directory.
	let directory = match &package.referent.node {
		tg::module::data::Source::Path(path) => tokio::fs::metadata(path)
			.await
			.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to get the metadata of the runtime"),
			)?
			.is_dir(),
		tg::module::data::Source::Edge(tg::graph::data::Edge::Index(_)) => {
			return Err(tg::error!("missing graph"));
		},
		tg::module::data::Source::Edge(tg::graph::data::Edge::Object(id)) => {
			id.kind() == tg::object::Kind::Directory
		},
		tg::module::data::Source::Edge(tg::graph::data::Edge::Pointer(pointer)) => {
			pointer.kind == tg::artifact::Kind::Directory
		},
	};
	if !directory {
		return Ok(None);
	}

	Ok(Some(package))
}

/// Get the modules of the interface files on disk with a stem, in every language, whether or not they exist.
fn path_interface_modules(stem: &std::path::Path) -> Vec<tg::module::Data> {
	tg::interface::Language::ALL
		.into_iter()
		.map(|language| {
			let mut name = stem.as_os_str().to_owned();
			name.push(format!(".{}", language.extension()));
			tg::module::Data {
				kind: language.kind(),
				referent: tg::Referent::with_node(tg::module::data::Source::Path(name.into())),
			}
		})
		.collect()
}
