use {crate as tg, std::collections::BTreeSet};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Location {
	#[tangram_serialize(id = 0)]
	pub module: tg::module::Data,

	#[tangram_serialize(id = 1)]
	pub range: tg::Range,
}

impl Location {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		self.module.children(children);
	}
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
