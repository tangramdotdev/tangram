use {crate::prelude::*, lsp_types as lsp, std::collections::BTreeSet};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Diagnostic {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::module::location::Data>,

	#[tangram_serialize(id = 1)]
	pub message: String,

	#[tangram_serialize(id = 2)]
	pub severity: Severity,
}

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::IsVariant,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[tangram_serialize(display, from_str)]
pub enum Severity {
	Error,
	Warning,
	Info,
	Hint,
}

impl Diagnostic {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(location) = &self.location {
			location.children(children);
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
