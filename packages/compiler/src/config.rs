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
			// Build the config's interface export, and resolve its imports relative to the config.
			let value = compiler.build_export(&arg.module, "interface").await?;
			let mut interface = tg::Interface::try_from_value(&value).map_err(|error| {
				tg::error!(
					!error,
					"failed to get the interface from the config's runtime"
				)
			})?;
			compiler
				.resolve_interface_imports(&arg.module, &mut interface)
				.await?;
			Ok(interface)
		}
		.boxed()
	}
}

impl Compiler {
	/// Resolve the imports of a config's interface whose paths are relative to the config, like the config's dependencies.
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
}
