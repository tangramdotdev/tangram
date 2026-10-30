use {super::Compiler, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub modules: Vec<tg::module::Data>,
}

#[derive(Debug)]
pub struct Output {
	pub diagnostics: Vec<tg::diagnostic::Data>,
}

impl Compiler {
	/// Get all diagnostics for the provided modules.
	pub async fn check(&self, modules: Vec<tg::module::Data>) -> tg::Result<Vec<tg::Diagnostic>> {
		// Check the modules of each language with its backend.
		let (config, modules): (Vec<_>, Vec<_>) = modules
			.into_iter()
			.partition(|module| module.kind.is_config() || module.kind.is_interface());
		let (python, javascript): (Vec<_>, Vec<_>) = modules
			.into_iter()
			.partition(|module| module.kind == tg::module::Kind::Python);
		let mut diagnostics = Vec::new();
		for (kind, modules) in [
			(tg::module::Kind::Json, config),
			(tg::module::Kind::Python, python),
			(tg::module::Kind::TypeScript, javascript),
		] {
			if modules.is_empty() {
				continue;
			}
			let arg = Arg { modules };
			let output = self.backend(kind).check(self, arg).await?;
			for diagnostic in output.diagnostics {
				diagnostics.push(diagnostic.try_into()?);
			}
		}

		Ok(diagnostics)
	}

	/// Get the modules whose interfaces have a check export.
	pub async fn check_exports(
		&self,
		modules: &[tg::module::Data],
	) -> tg::Result<Vec<tg::module::Data>> {
		let mut output = Vec::new();
		for module in modules {
			let checkable = module.kind.is_config()
				|| matches!(
					module.kind,
					tg::module::Kind::JavaScript
						| tg::module::Kind::Python
						| tg::module::Kind::TypeScript
				);
			if !checkable {
				continue;
			}
			#[cfg(all(feature = "javascript", feature = "python"))]
			if self.interface(module).await?.exports.contains_key("check") {
				output.push(module.clone());
			}
		}
		Ok(output)
	}
}
