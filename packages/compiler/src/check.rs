use {
	super::{Compiler, document},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub modules: Vec<tg::module::Data>,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct Response {
	pub diagnostics: Vec<tg::diagnostic::Data>,
	/// The modules visited by the checker, including dependencies.
	pub modules: Vec<tg::module::Data>,
}

impl Compiler {
	/// Get all diagnostics for the provided modules.
	pub async fn check(&self, modules: Vec<tg::module::Data>) -> tg::Result<Vec<tg::Diagnostic>> {
		// Check the Python modules.
		let (python, modules): (Vec<_>, Vec<_>) = modules
			.into_iter()
			.partition(|module| module.kind == tg::module::Kind::Python);
		#[cfg(not(feature = "python"))]
		if !python.is_empty() {
			return Err(tg::error!("the python feature is not enabled"));
		}
		#[cfg(not(feature = "python"))]
		let mut response = Response::default();
		#[cfg(feature = "python")]
		let mut response = if python.is_empty() {
			Response::default()
		} else {
			self.check_python(python).await?
		};

		// Check the JavaScript modules.
		if !modules.is_empty() {
			let request = super::Request::Check(Request { modules });
			let javascript = self.request(request).await?.unwrap_check();
			response.diagnostics.extend(javascript.diagnostics);
			response.modules.extend(javascript.modules);
		}

		// Warn about exports throughout the checked dependency graph.
		let mut diagnostics = self
			.get_export_diagnostics(&response.modules, tg::position::Encoding::Utf8)
			.await?;
		let checked = response
			.diagnostics
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<Vec<_>>>()?;

		diagnostics.extend(checked);

		Ok(diagnostics)
	}

	/// Warn about exports that another language must rename or cannot bind.
	pub(crate) async fn get_export_diagnostics(
		&self,
		modules: &[tg::module::Data],
		encoding: tg::position::Encoding,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		let mut diagnostics = Vec::new();
		if !cfg!(feature = "python") {
			return Ok(diagnostics);
		}
		let mut visited = BTreeSet::new();
		for module in modules {
			if !matches!(
				module.kind,
				tg::module::Kind::JavaScript | tg::module::Kind::TypeScript
			) || !visited.insert(document::Key::new(module))
			{
				continue;
			}
			// The checker reports a module that cannot be loaded.
			let Ok(text) = self.load_module(module).await else {
				continue;
			};
			for diagnostic in super::load::diagnostics(module, &text, encoding) {
				diagnostics.push(diagnostic.try_into()?);
			}
		}
		Ok(diagnostics)
	}
}
