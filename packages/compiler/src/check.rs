use {
	super::{Compiler, document},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub modules: Vec<tg::module::Data>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub diagnostics: Vec<tg::diagnostic::Data>,
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
		let mut diagnostics = Vec::new();
		#[cfg(feature = "python")]
		let mut diagnostics = if python.is_empty() {
			Vec::new()
		} else {
			self.check_python(python).await?
		};
		if modules.is_empty() {
			return Ok(diagnostics);
		}

		// Create the request.
		let request = super::Request::Check(Request { modules });

		// Perform the request.
		let response = self.request(request).await?.unwrap_check();

		// Convert diagnostics from data to the non-serializable form.
		let javascript = response
			.diagnostics
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<Vec<_>>>()?;

		diagnostics.extend(javascript);

		// Remove the diagnostics that both checkers report, such as warnings about the exports of a module that both reach.
		let mut reported = BTreeSet::new();
		diagnostics.retain(|diagnostic| {
			let data = diagnostic.to_data();
			let location = data.location.map(|location| {
				let tg::Range { start, end } = location.range;
				let key = document::Key::new(&location.module);
				(key, start.line, start.character, end.line, end.character)
			});
			reported.insert((location, data.message))
		});

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
		for module in modules {
			if !matches!(
				module.kind,
				tg::module::Kind::JavaScript | tg::module::Kind::TypeScript
			) {
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
