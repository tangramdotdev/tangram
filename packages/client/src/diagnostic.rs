use crate as tg;
use lsp_types as lsp;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
	pub location: Option<tg::Location>,
	pub severity: Severity,
	pub message: String,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Severity {
	Error,
	Warning,
	Information,
	Hint,
}

impl From<Diagnostic> for lsp::Diagnostic {
	fn from(value: Diagnostic) -> Self {
		let range = value
			.location
			.map(|location| location.range.into())
			.unwrap_or_default();
		let severity = Some(value.severity.into());
		let source = Some("tangram".to_owned());
		let message = value.message;
		Self {
			range,
			severity,
			source,
			message,
			..Default::default()
		}
	}
}

impl From<Severity> for lsp::DiagnosticSeverity {
	fn from(value: Severity) -> Self {
		match value {
			Severity::Error => lsp::DiagnosticSeverity::ERROR,
			Severity::Warning => lsp::DiagnosticSeverity::WARNING,
			Severity::Information => lsp::DiagnosticSeverity::INFORMATION,
			Severity::Hint => lsp::DiagnosticSeverity::HINT,
		}
	}
}
