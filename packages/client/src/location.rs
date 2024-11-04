use crate as tg;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	pub module: tg::module::Data,
	pub range: tg::Range,
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.module)?;
		let line = self.range.start.line + 1;
		let character = self.range.start.character + 1;
		write!(f, ":{line}:{character}")?;
		Ok(())
	}
}
