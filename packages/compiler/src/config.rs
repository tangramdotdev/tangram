use {
	crate::{Compiler, check},
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
			if let Some(name) = tg::module::config::interface_file_name(path) {
				let path = path.with_file_name(name);
				modules.push(tg::module::Data {
					kind: tg::module::Kind::Interface,
					referent: tg::Referent::with_node(tg::module::data::Source::Path(path)),
				});
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
			let path = path.join(tg::module::config::RUNTIME_INTERFACE_FILE_NAME);
			modules.push(tg::module::Data {
				kind: tg::module::Kind::Interface,
				referent: tg::Referent::with_node(tg::module::data::Source::Path(path)),
			});
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
		let mut interface = tg::interface::parse::parse(&text)?;
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
	// Get the file name of the interface.
	let path = match &module.referent.node {
		tg::module::data::Source::Edge(_) => module.referent.path(),
		tg::module::data::Source::Path(path) => Some(path.as_path()),
	};
	let Some(name) = path.and_then(tg::module::config::interface_file_name) else {
		return Ok(None);
	};
	let reference = format!("./{name}").parse::<tg::Reference>()?;

	// Determine whether the config imports the interface.
	let exists = match &module.referent.node {
		tg::module::data::Source::Path(path) => tokio::fs::try_exists(path.with_file_name(&name))
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to determine whether the interface exists")
			})?,
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
			file.dependencies_with_instance(instance)
				.await?
				.contains_key(&reference)
		},
	};
	if !exists {
		return Ok(None);
	}

	// Resolve the interface.
	let import = tg::module::Import {
		kind: Some(tg::module::Kind::Interface),
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
	let name = tg::module::config::RUNTIME_INTERFACE_FILE_NAME;

	// Get the interface in the package.
	let referent = match &package.referent.node {
		tg::module::data::Source::Path(path) => {
			let path = path.join(name);
			let exists = tokio::fs::try_exists(&path).await.map_err(|error| {
				tg::error!(!error, "failed to determine whether the interface exists")
			})?;
			if !exists {
				return Ok(None);
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
				.try_get_entry_edge_with_instance(instance, name)
				.await?
			else {
				return Ok(None);
			};
			let edge = tg::graph::Edge::<tg::Object>::from(edge).to_data();
			let path = package
				.referent
				.path()
				.map_or_else(|| name.into(), |path| path.join(name));
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
	let module = tg::module::Data {
		kind: tg::module::Kind::Interface,
		referent,
	};

	Ok(Some(module))
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
