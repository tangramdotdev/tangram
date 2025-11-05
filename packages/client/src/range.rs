use {crate::prelude::*, lsp_types as lsp};

/// A range in a string.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Range {
	#[tangram_serialize(id = 0)]
	pub start: tg::Position,
	#[tangram_serialize(id = 1)]
	pub end: tg::Position,
}

impl Range {
	#[must_use]
	pub fn try_from_byte_range_in_string(
		string: &str,
		range: std::ops::Range<usize>,
		encoding: tg::position::Encoding,
	) -> Option<Self> {
		let start = tg::Position::try_from_byte_index_in_string(string, range.start, encoding)?;
		let end = tg::Position::try_from_byte_index_in_string(string, range.end, encoding)?;
		Some(Self { start, end })
	}

	#[must_use]
	pub fn try_to_byte_range_in_string(
		self,
		string: &str,
		encoding: tg::position::Encoding,
	) -> Option<std::ops::Range<usize>> {
		let start = self.start.try_to_byte_index_in_string(string, encoding)?;
		let end = self.end.try_to_byte_index_in_string(string, encoding)?;
		Some(start..end)
	}
}

impl From<Range> for lsp::Range {
	fn from(value: Range) -> Self {
		Self {
			start: value.start.into(),
			end: value.end.into(),
		}
	}
}

impl From<lsp::Range> for Range {
	fn from(value: lsp::Range) -> Self {
		Self {
			start: value.start.into(),
			end: value.end.into(),
		}
	}
}
