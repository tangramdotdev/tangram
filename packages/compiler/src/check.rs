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
		let roots = modules.clone();
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

		// Build the check export of each module that has one, after its code checks without errors. A config module's runtime handles its check export.
		if diagnostics.is_empty() {
			for module in &roots {
				if self.has_check_export(module).await? {
					self.build_export(module, "check").await?;
				}
			}
		}

		Ok(diagnostics)
	}

	/// Determine whether a module has a check export, from the exports of its interface.
	async fn has_check_export(&self, module: &tg::module::Data) -> tg::Result<bool> {
		if !(module.kind.is_config()
			|| matches!(
				module.kind,
				tg::module::Kind::JavaScript
					| tg::module::Kind::Python
					| tg::module::Kind::TypeScript
			)) {
			return Ok(false);
		}
		#[cfg(all(feature = "javascript", feature = "python"))]
		let check = self.interface(module).await?.exports.contains_key("check");
		#[cfg(not(all(feature = "javascript", feature = "python")))]
		let check = false;
		Ok(check)
	}
}
