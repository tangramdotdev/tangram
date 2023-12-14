use super::{Module, Range};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
	pub module: Module,
	pub range: Range,
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"{}:{}:{}",
			self.module,
			self.range.start.line + 1,
			self.range.start.character + 1,
		)
	}
}
