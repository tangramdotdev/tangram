use {super::Compiler, itertools::Itertools as _, lsp_types as lsp, tangram_client as tg};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentRequest {
	modules: Vec<tg::module::Data>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentResponse {
	pub diagnostics: Vec<tg::Diagnostic>,
}

impl Compiler {
	pub async fn get_document_diagnostics(
		&self,
		modules: Vec<tg::module::Data>,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		// Create the request.
		let request = super::Request::DocumentDiagnostics(DocumentRequest { modules });

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::DocumentDiagnostics(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};
		let DocumentResponse { diagnostics } = response;

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
