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
}
