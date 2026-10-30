use {
	crate::{Compiler, check},
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
};

/// The backend for config modules, whose runtimes run them, and for interface modules, which declare the interfaces of config modules.
pub(crate) struct Backend;

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
		let Some(module) = self.try_get_config_interface_module(module).await? else {
			return Ok(tg::Interface::default());
		};
		let interface = self.read_interface(&module).await?;
		Ok(interface)
	}

	/// Get the interface module of a config: the interface beside it, or else its runtime's interface for configs.
	pub(crate) async fn try_get_config_interface_module(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<Option<tg::module::Data>> {
		if let Some(module) = self.try_get_config_sibling_interface_module(module).await? {
			return Ok(Some(module));
		}
		let module = self.try_get_config_runtime_interface_module(module).await?;
		Ok(module)
	}

	/// Get the module of the interface beside a config, which the config imports.
	async fn try_get_config_sibling_interface_module(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<Option<tg::module::Data>> {
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
			tg::module::data::Source::Path(path) => {
				tokio::fs::try_exists(path.with_file_name(&name))
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to determine whether the interface exists")
					})?
			},
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
				file.dependencies_with_instance(&self.instance)
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
		let output =
			self.instance.resolve_module(arg).await.map_err(|error| {
				tg::error!(!error, "failed to resolve the interface of the config")
			})?;

		Ok(Some(output.module))
	}

	/// Get the module of the interface for configs in a config's runtime package, if the runtime is a package that has one.
	pub(crate) async fn try_get_config_runtime_interface_module(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<Option<tg::module::Data>> {
		// Resolve the runtime's package.
		let text = self.load_module(module).await?;
		let format = tg::module::config::Format::for_kind(module.kind).unwrap();
		let config = tg::module::Config::parse(&text, format)?;
		let mut import = config.runtime.import()?;
		import.kind = Some(tg::module::Kind::Directory);
		let arg = tg::module::resolve::Arg {
			import,
			referrer: Some(module.clone()),
		};
		let Ok(output) = self.instance.resolve_module(arg).await else {
			return Ok(None);
		};
		let package = output.module;
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
					.try_get_entry_edge_with_instance(&self.instance, name)
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

	/// Read an interface module, and resolve its imports whose paths are relative to it.
	pub(crate) async fn read_interface(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<tg::Interface> {
		let text = self.load_module(module).await?;
		let mut interface = tg::Interface::deserialize_json(&text)
			.map_err(|error| tg::error!(argument, !error, "invalid interface"))?;
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
