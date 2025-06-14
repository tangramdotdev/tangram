use crate as tg;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	pub module: tg::module::Data,
	pub range: tg::Range,
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.module)?;
		let start_line = self.range.start.line + 1;
		let start_character = self.range.start.character + 1;
		let end_line = self.range.end.line + 1;
		let end_character = self.range.end.character + 1;
		write!(
			f,
			":{start_line}:{start_character}-{end_line}:{end_character}"
		)?;
		Ok(())
	}
}
