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
			// Validate each config.
			for module in &arg.modules {
				let text = compiler.load_module(module).await?;
				Compiler::analyze(module, &text)?;
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
				let mut interface = tg::Interface::deserialize_json(&text)
					.map_err(|error| tg::error!(argument, !error, "invalid interface"))?;
				compiler
					.resolve_interface_imports(&arg.module, &mut interface)
					.await?;
				return Ok(interface);
			}

			// Otherwise, build the runtime's interface export.
			let value = compiler.build_export(&arg.module, "interface").await?;
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
	/// Resolve the imports of an interface beside a config whose paths are relative to the config, like the config's dependencies.
	async fn resolve_interface_imports(
		&self,
		module: &tg::module::Data,
		interface: &mut tg::Interface,
	) -> tg::Result<()> {
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
		Ok(())
	}

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
}
