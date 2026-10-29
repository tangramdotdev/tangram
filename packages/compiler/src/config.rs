use {
	crate::{Compiler, check},
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
};

/// The backend for config modules, whose runtimes answer queries about them.
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
			// Validate each config, and then run its runtime's check export.
			for module in &arg.modules {
				let text = compiler.load_module(module).await?;
				Compiler::analyze(module, &text)?;
				compiler.runtime_export(module, "check").await?;
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
			// Read the interface beside the config, if there is one.
			if let Some(text) = compiler.try_read_interface_file(&arg.module).await? {
				let interface = tg::Interface::deserialize_json(&text)
					.map_err(|error| tg::error!(argument, !error, "invalid interface"))?;
				return Ok(interface);
			}

			// Otherwise, build the runtime's interface export.
			let value = compiler.runtime_export(&arg.module, "interface").await?;
			tg::Interface::try_from_value(&value).map_err(|error| {
				tg::error!(
					!error,
					"failed to get the interface from the config's runtime"
				)
			})
		}
		.boxed()
	}
}

impl Compiler {
	/// Read the interface beside a config, from the file system for a path or from the config's dependencies otherwise.
	async fn try_read_interface_file(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<Option<String>> {
		let path = match &module.referent.node {
			tg::module::data::Source::Edge(_) => module.referent.path(),
			tg::module::data::Source::Path(path) => Some(path.as_path()),
		};
		let Some(name) = path.and_then(tg::module::config::interface_file_name) else {
			return Ok(None);
		};
		if let tg::module::data::Source::Path(path) = &module.referent.node {
			let text = tokio::fs::read_to_string(path.with_file_name(name))
				.await
				.ok();
			return Ok(text);
		}
		let import = tg::module::Import {
			kind: Some(tg::module::Kind::File),
			reference: format!("./{name}").parse()?,
		};
		let arg = tg::module::resolve::Arg {
			import,
			referrer: Some(module.clone()),
		};
		let Ok(output) = self.instance.resolve_module(arg).await else {
			return Ok(None);
		};
		let module = tg::Module::try_from_data(output.module)?;
		let tg::module::Source::Edge(edge) = module.referent.node() else {
			return Ok(None);
		};
		let file = match edge.clone() {
			tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
			tg::graph::Edge::Object(object) => object
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected the interface to be a file"))?,
			tg::graph::Edge::Pointer(pointer) => tg::File::with_pointer(pointer),
		};
		let text = file.text_with_instance(&self.instance).await?;
		Ok(Some(text))
	}

	/// Refer to the package directory of a checked-in root config, whose referent names the containing root by ID and the config by its path within the root, so that the runtime receives the package.
	async fn config_package_module(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<tg::module::Data> {
		let Some(path) = module.referent.path() else {
			return Ok(module.clone());
		};
		let root = is_root_config(path);
		let Some(tg::object::Id::Directory(id)) =
			module.referent.options.id.clone().filter(|_| root)
		else {
			return Ok(module.clone());
		};
		let mut directory = tg::Directory::with_id(id);
		if let Some(parent) = path
			.parent()
			.filter(|parent| !parent.as_os_str().is_empty())
		{
			directory = directory
				.get_with_instance(&self.instance, parent)
				.await?
				.try_unwrap_directory()
				.map_err(|_| tg::error!("expected the package to be a directory"))?;
		}
		let edge = tg::graph::data::Edge::Object(directory.id().into());
		let mut referent = module
			.referent
			.clone()
			.map(|_| tg::module::data::Source::Edge(edge));
		referent.options.path = None;
		let module = tg::module::Data {
			kind: tg::module::Kind::Config,
			referent,
		};
		Ok(module)
	}

	/// Build an export of a config module's runtime, checking in the module first if it is a path.
	pub(crate) async fn runtime_export(
		&self,
		module: &tg::module::Data,
		export: &str,
	) -> tg::Result<tg::Value> {
		// Get a module that refers to an object, checking in a path.
		// Use the local path of a checked-in root config, so that its package is checked in.
		let local = match &module.referent.node {
			tg::module::data::Source::Edge(_) => module
				.referent
				.path()
				.filter(|path| path.is_absolute() && is_root_config(path) && path.is_file())
				.map(ToOwned::to_owned),
			tg::module::data::Source::Path(path) => Some(path.clone()),
		};
		let module = match &local {
			None => self.config_package_module(module).await?,
			Some(path) => {
				// Check in the package of a root config, so that the runtime receives the package, and the config file otherwise.
				let root = is_root_config(path);
				let checkin_path = if root {
					path.parent().unwrap().to_owned()
				} else {
					path.clone()
				};
				let arg = tg::checkin::Arg {
					options: tg::checkin::Options::default(),
					path: checkin_path,
					updates: Vec::new(),
				};
				let output = tg::checkin::checkin_with_instance(&self.instance, arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to check in the config"))?;
				let edge = tg::graph::data::Edge::Object(output.artifact.node.clone().into());
				let mut referent = output
					.artifact
					.map(|_| tg::module::data::Source::Edge(edge));
				if !root {
					referent.options.path = path.file_name().map(Into::into);
				}
				tg::module::Data {
					kind: tg::module::Kind::Config,
					referent,
				}
			},
		};
		let module = tg::Module::try_from_data(module)?;

		// Build the runtime's export.
		let args = [
			"builtin".into(),
			"config".into(),
			"--runtime-export".into(),
			export.into(),
			tg::command::Value::Value(module.into()),
		];
		let executable = tg::command::Executable {
			artifact: None,
			path: Some("tg".into()),
		};
		let command = tg::command::Builder::new()
			.args(args)
			.executable(executable)
			.build()?;
		let command = tg::Referent::with_node(tg::Either::Right(command));
		let arg = tg::process::Arg {
			command: Some(command),
			stderr: tg::process::Stdio::Log,
			stdin: tg::process::Stdio::Null,
			stdout: tg::process::Stdio::Log,
			..Default::default()
		};
		// Build on a blocking thread, because the build future is not sendable.
		let instance = self.instance.clone();
		let handle = tokio::runtime::Handle::current();
		let output = tokio::task::spawn_blocking(move || {
			handle.block_on(tg::process::build::build_with_instance(&instance, arg))
		})
		.await
		.map_err(|error| tg::error!(!error, "the build task panicked"))?
		.map_err(
			|error| tg::error!(!error, %export, "failed to build the export of the config's runtime"),
		)?;

		Ok(output)
	}
}

/// Determine whether a path names the root config of a package.
fn is_root_config(path: &std::path::Path) -> bool {
	path.file_name()
		.and_then(|name| name.to_str())
		.is_some_and(|name| {
			tg::module::ROOT_MODULE_FILE_NAMES.contains(&name)
				&& tg::module::config::Format::for_path(name).is_some()
		})
}
