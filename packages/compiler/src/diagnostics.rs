use {super::Compiler, itertools::Itertools as _, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct DocumentArg {
	pub modules: Vec<tg::module::Data>,
}

#[derive(Debug)]
pub struct DocumentOutput {
	pub diagnostics: Vec<tg::diagnostic::Data>,
}

impl Compiler {
	pub async fn get_document_diagnostics(
		&self,
		modules: Vec<tg::module::Data>,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		// Get the diagnostics for the modules of each language from its backend.
		let (python, javascript): (Vec<_>, Vec<_>) = modules
			.into_iter()
			.partition(|module| module.kind == tg::module::Kind::Python);
		let mut diagnostics = Vec::new();
		for (kind, modules) in [
			(tg::module::Kind::Python, python),
			(tg::module::Kind::TypeScript, javascript),
		] {
			if modules.is_empty() {
				continue;
			}
			let arg = DocumentArg { modules };
			let output = self.backend(kind).document_diagnostics(self, arg).await?;
			for diagnostic in output.diagnostics {
				diagnostics.push(diagnostic.try_into()?);
			}
		}

		Ok(diagnostics)
	}
}

impl Compiler {
	pub(super) async fn handle_document_diagnostic_request(
		&self,
		params: lsp::DocumentDiagnosticParams,
	) -> tg::Result<lsp::DocumentDiagnosticReportResult> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the diagnostics.
		let diagnostics = self
			.get_document_diagnostics(vec![module])
			.await?
			.into_iter()
			.map_into()
			.collect();

		Ok(lsp::DocumentDiagnosticReportResult::Report(
			lsp::DocumentDiagnosticReport::Full(lsp::RelatedFullDocumentDiagnosticReport {
				full_document_diagnostic_report: lsp::FullDocumentDiagnosticReport {
					items: diagnostics,
					..Default::default()
				},
				..Default::default()
			}),
		))
	}
}
