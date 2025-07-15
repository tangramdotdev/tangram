use crate as tg;
use lsp_types as lsp;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Diagnostic {
	pub location: Option<tg::Location>,
	pub severity: Severity,
	pub message: String,
}

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::IsVariant,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Severity {
	Error,
	Warning,
	Info,
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
			Severity::Info => lsp::DiagnosticSeverity::INFORMATION,
			Severity::Hint => lsp::DiagnosticSeverity::HINT,
		}
	}
}

impl std::fmt::Display for Severity {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Error => write!(f, "error"),
			Self::Warning => write!(f, "warning"),
			Self::Info => write!(f, "info"),
			Self::Hint => write!(f, "hint"),
		}
	}
}

impl std::str::FromStr for Severity {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"error" => Ok(Self::Error),
			"warning" => Ok(Self::Warning),
			"info" => Ok(Self::Info),
			"hint" => Ok(Self::Hint),
			_ => Err(tg::error!(%kind = s, "invalid severity")),
		}
	}
}

impl std::fmt::Display for Diagnostic {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let Self {
			severity,
			message,
			location,
		} = self;
		write!(f, "{severity} {message}")?;
		if let Some(location) = location {
			write!(f, " {location}")?;
		}
		Ok(())
	}
}
