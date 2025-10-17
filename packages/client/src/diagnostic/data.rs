use {crate as tg, lsp_types as lsp, std::collections::BTreeSet};

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
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Data>,

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
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
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
