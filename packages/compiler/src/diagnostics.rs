use {super::Compiler, itertools::Itertools as _, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct DocumentRequest {
	pub modules: Vec<tg::module::Data>,
}

#[derive(Debug, serde::Deserialize)]
pub struct DocumentResponse {
	pub diagnostics: Vec<tg::diagnostic::Data>,
}

impl Compiler {
	pub async fn get_document_diagnostics(
		&self,
		modules: Vec<tg::module::Data>,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		let (python, modules): (Vec<_>, Vec<_>) = modules
			.into_iter()
			.partition(|module| module.kind == tg::module::Kind::Py);
		let mut diagnostics = Vec::new();
		#[cfg(not(feature = "py"))]
		if !python.is_empty() {
			return Err(tg::error!("the py feature is not enabled"));
		}
		#[cfg(feature = "py")]
		if !python.is_empty() {
			let request = super::Request::DocumentDiagnostics(DocumentRequest { modules: python });
			let response = self
				.request_py(request)
				.await?
				.unwrap_document_diagnostics();
			diagnostics.extend(
				response
					.diagnostics
					.into_iter()
					.map(TryInto::try_into)
					.collect::<tg::Result<Vec<_>>>()?,
			);
		}
		if modules.is_empty() {
			return Ok(diagnostics);
		}
		// Create the request.
		let request = super::Request::DocumentDiagnostics(DocumentRequest { modules });

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::DocumentDiagnostics(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};
		let DocumentResponse {
			diagnostics: javascript,
		} = response;

		let javascript = javascript
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<Vec<_>>>()?;

		diagnostics.extend(javascript);
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
