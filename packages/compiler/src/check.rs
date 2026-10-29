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
			.partition(|module| module.kind == tg::module::Kind::Config);
		let (python, javascript): (Vec<_>, Vec<_>) = modules
			.into_iter()
			.partition(|module| module.kind == tg::module::Kind::Python);
		let mut diagnostics = Vec::new();
		for (kind, modules) in [
			(tg::module::Kind::Config, config),
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

	/// Determine whether a module has a check export, which a config module always has, because its runtime handles it.
	async fn has_check_export(&self, module: &tg::module::Data) -> tg::Result<bool> {
		match module.kind {
			tg::module::Kind::Config => Ok(true),
			tg::module::Kind::JavaScript
			| tg::module::Kind::Python
			| tg::module::Kind::TypeScript => {
				let text = self.load_module(module).await?;
				let interface = crate::interface::module(module, &text)?;
				Ok(interface.exports.contains_key("check"))
			},
			_ => Ok(false),
		}
	}
}
