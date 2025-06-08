use crate as tg;
use lsp_types as lsp;

/// A range in a string.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Range {
	pub start: tg::Position,
	pub end: tg::Position,
}

impl Range {
	#[must_use]
	pub fn from_byte_range_in_string(string: &str, byte_range: std::ops::Range<usize>) -> Self {
		let start = tg::Position::try_from_byte_index_in_string(string, byte_range.start)
			.unwrap_or_else(|| tg::Position::past_end(string));
		let end = tg::Position::try_from_byte_index_in_string(string, byte_range.end)
			.unwrap_or_else(|| tg::Position::past_end(string));
		Self { start, end }
	}

	#[must_use]
	pub fn to_byte_range_in_string(self, string: &str) -> std::ops::Range<usize> {
		let start = self
			.start
			.try_to_byte_index_in_string(string)
			.unwrap_or(string.len());
		let end = self
			.end
			.try_to_byte_index_in_string(string)
			.unwrap_or(string.len());
		start..end
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
