use crate as tg;

pub use self::data::Location as Data;

pub mod data;

#[derive(Clone, Debug)]
pub struct Location {
	pub module: tg::Module,
	pub range: tg::Range,
}

impl Location {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.module.children()
	}

	#[must_use]
	pub fn to_data(&self) -> data::Location {
		let module = self.module.to_data();
		let range = self.range;
		data::Location { module, range }
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let module = data.module.into();
		let range = data.range;
		Ok(Self { module, range })
	}
}

impl TryFrom<data::Location> for Location {
	type Error = tg::Error;

	fn try_from(value: data::Location) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
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
