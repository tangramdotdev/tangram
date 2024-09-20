use crate as tg;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	pub module: tg::Module,
	pub range: tg::Range,
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.module.kind)?;
		if let Some(object) = &self.module.object {
			let object = object.as_ref().map_right(|path| path.display());
			write!(f, ":{object}")?;
		}
		if let Some(path) = &self.module.path {
			write!(f, ":{}", path.display())?;
		}
		let line = self.range.start.line + 1;
		let character = self.range.start.character + 1;
		write!(f, " {line}:{character}")?;
		Ok(())
	}
}
