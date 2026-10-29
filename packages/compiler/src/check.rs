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

		Ok(diagnostics)
	}
}
