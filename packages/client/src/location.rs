use crate as tg;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
	pub module: tg::Module,
	pub range: tg::Range,
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
