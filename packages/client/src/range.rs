use crate as tg;
use lsp_types as lsp;

/// A range in a string, such as a text editor selection. The end is exclusive. This type maps cleanly to the `Range` type in the Language Server Protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Range {
	pub start: tg::Position,
	pub end: tg::Position,
}

impl Range {
	/// Convert a byte range of the given string to a `Range`. If the end is out of bounds, the range will end at the start of the next line past the end of the string. If both stard and end are out of bounds, then the returned range will start and end at the next line past the end of the string.
	#[must_use]
	pub fn from_byte_range_in_string(string: &str, byte_range: std::ops::Range<usize>) -> Self {
		let start = tg::Position::try_from_byte_index_in_string(string, byte_range.start)
			.unwrap_or_else(|| tg::Position::past_end(string));
		let end = tg::Position::try_from_byte_index_in_string(string, byte_range.end)
			.unwrap_or_else(|| tg::Position::past_end(string));

		Self { start, end }
	}

	/// Convert `self` to a byte range within the given string. If the end is out of bounds, the end of the returned range will be `string.len()` (exclusive). If both stard and end are out of bounds, then the returned range will be `string.len()..string.len()`.
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

#[allow(clippy::ignored_unit_patterns)]
#[cfg(test)]
mod tests {
	use super::*;
	use proptest::prelude::*;

	// Get a random non-empty string with a random index. Note that the index may not be a character boundary.
	prop_compose! {
		fn string_with_index()(string in ".+")(offset in 0..string.len(), string in Just(string)) -> (String, usize) {
			(string, offset)
		}
	}

	// Get a random non-empty string with a random index. Note that the index may not be a character boundary.
	prop_compose! {
		fn string_with_range()((string, end) in string_with_index())(start in 0..=end, end in Just(end), string in Just(string)) -> (String, std::ops::Range<usize>) {
			(string, start..end)
		}
	}

	proptest! {
		#[test]
		fn range_to_byte_range_round_trip((string, byte_range) in string_with_range()) {
			prop_assume!(string.is_char_boundary(byte_range.start));
			prop_assume!(string.is_char_boundary(byte_range.end));

			let range = Range::from_byte_range_in_string(&string, byte_range.clone());
			let new_byte_range = range.to_byte_range_in_string(&string);

			prop_assert_eq!(byte_range, new_byte_range);
		}
	}

	#[test]
	fn test_full_and_empty_ranges() {
		let string = "Hello 👋 World\nHi 🌎!";

		let empty_byte_range_1 = 0..0;
		let empty_byte_range_2 = 0..1;
		let full_byte_range = 0..string.len();

		assert_eq!(
			Range::from_byte_range_in_string(string, empty_byte_range_1),
			Range {
				start: tg::Position {
					line: 0,
					character: 0
				},
				end: tg::Position {
					line: 0,
					character: 0
				}
			}
		);
		assert_eq!(
			Range::from_byte_range_in_string(string, empty_byte_range_2),
			Range {
				start: tg::Position {
					line: 0,
					character: 0,
				},
				end: tg::Position {
					line: 0,
					character: 1,
				},
			}
		);

		// The end of a full range should be the start of a new line.
		assert_eq!(
			Range::from_byte_range_in_string(string, full_byte_range),
			Range {
				start: tg::Position {
					line: 0,
					character: 0,
				},
				end: tg::Position {
					line: 2,
					character: 0,
				},
			}
		);
	}

	#[test]
	fn test_end_ranges() {
		let string = "Hello 👋 World\nHi 🌎!";

		let full_range = Range {
			start: tg::Position {
				line: 0,
				character: 0,
			},
			end: tg::Position {
				line: 2,
				character: 0,
			},
		};

		let insert_at_end_range = Range {
			start: tg::Position {
				line: 2,
				character: 0,
			},
			end: tg::Position {
				line: 2,
				character: 0,
			},
		};

		assert_eq!(full_range.to_byte_range_in_string(string), 0..string.len());
		assert_eq!(
			Range::from_byte_range_in_string(string, 0..string.len()),
			full_range,
		);

		assert_eq!(
			insert_at_end_range.to_byte_range_in_string(string),
			string.len()..string.len(),
		);
		assert_eq!(
			Range::from_byte_range_in_string(string, string.len()..string.len()),
			insert_at_end_range,
		);
	}

	#[test]
	fn test_empty_string_range() {
		let string = "";

		let full_range = Range {
			start: tg::Position {
				line: 0,
				character: 0,
			},
			end: tg::Position {
				line: 1,
				character: 0,
			},
		};

		// Dealing with ranges for an empty string is a bit fuzzy, and is currently "lossy".
		assert_eq!(full_range.to_byte_range_in_string(string), 0..0);
		assert_eq!(
			Range::from_byte_range_in_string(string, 0..0),
			Range {
				start: tg::Position {
					line: 1,
					character: 0
				},
				end: tg::Position {
					line: 1,
					character: 0
				},
			}
		);
	}
}
