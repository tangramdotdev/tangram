use lsp_types as lsp;

/// A position in a string, identified by zero-indexed line and character offsets. This type maps cleanly to the `Position` type in the Language Server Protocol. For maximum compatibility with the Language Server Protocol, character offsets use UTF-16 code units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
	pub line: u32,
	pub character: u32,
}

impl Position {
	/// Convert a byte index within the given string to a position. If the index is out of bounds, then this function returns `None`. The byte index must be on a character boundary.
	#[must_use]
	pub fn try_from_byte_index_in_string(string: &str, index: usize) -> Option<Self> {
		let mut line = 0;
		let mut character = 0;
		for (char_index, c) in string.char_indices() {
			assert!(
				char_index <= index,
				"string index {index} is not on a character boundary"
			);

			if char_index == index {
				return Some(Self { line, character });
			}

			if c == '\n' {
				line += 1;
				character = 0;
			} else {
				let len: u32 = c.len_utf16().try_into().unwrap();
				character += len;
			}
		}

		None
	}

	/// Convert a byte index within the given string to a position. If the index is out of bounds, then this function panics. See [`Self::try_from_byte_index_in_string`] for more details.
	#[must_use]
	pub fn from_byte_index_in_string(string: &str, index: usize) -> Self {
		Self::try_from_byte_index_in_string(string, index).unwrap_or_else(|| {
			panic!(
				"string index {index} is out of bounds of string with length {}",
				string.len()
			);
		})
	}

	/// Convert `self` to a byte index within the given string. If the position is out of bounds, then this function returns `None`. The returned index will always fall on a character boundary in `string`.
	#[must_use]
	pub fn try_to_byte_index_in_string(self, string: &str) -> Option<usize> {
		let mut line = 0;
		let mut character = 0;
		for (index, c) in string.char_indices() {
			if line >= self.line && character >= self.character {
				return Some(index);
			}

			if c == '\n' {
				line += 1;
				character = 0;
			} else {
				let len: u32 = c.len_utf16().try_into().unwrap();
				character += len;
			}
		}

		None
	}

	/// Convert `self` to a byte index within the given string. If the position is out of bounds, then this function panics. See [`Self::try_to_byte_index_in_string`] for more details.
	#[must_use]
	pub fn to_byte_index_in_string(self, string: &str) -> usize {
		self.try_to_byte_index_in_string(string).unwrap_or_else(|| {
			panic!(r#"position "{self:?}" is out of bounds of the string"#);
		})
	}

	/// Get the end position of a string. This position will be at the start of the line after the last line of the string, and this position is not considered in bounds. This is useful for representing the point to insert at the end of the string, or for creating a range to cover the full string. Conceptually, this would be similar to getting the position for `string.len()` (though such a position would be considered out of bounds).
	#[must_use]
	pub fn past_end(string: &str) -> Self {
		let lines = string.split('\n');
		let num_lines = lines.count();
		let num_lines: u32 = num_lines.try_into().unwrap();

		Self {
			line: num_lines,
			character: 0,
		}
	}
}

impl From<Position> for lsp::Position {
	fn from(value: Position) -> Self {
		Self {
			line: value.line,
			character: value.character,
		}
	}
}

impl From<lsp::Position> for Position {
	fn from(value: lsp::Position) -> Self {
		Position {
			line: value.line,
			character: value.character,
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

	proptest! {
		#[test]
		fn position_to_offset_round_trip((string, index) in string_with_index()) {
			prop_assume!(string.is_char_boundary(index));

			let position = Position::from_byte_index_in_string(&string, index);
			let new_offset = position.to_byte_index_in_string(&string);

			prop_assert_eq!(index, new_offset);
		}
	}

	#[test]
	fn positions_and_offsets() {
		let string = "Hello 👋 World\nHi 🌎!";

		let offsets = [
			(0, (0, 0)),   // H
			(1, (0, 1)),   // e
			(6, (0, 6)),   // 👋
			(10, (0, 8)),  // " "
			(11, (0, 9)),  // W
			(16, (0, 14)), // \n
			(17, (1, 0)),  // H
			(18, (1, 1)),  // i
			(20, (1, 3)),  // 🌎
			(24, (1, 5)),  // !
		];

		for (offset, (line, character)) in &offsets {
			let position = Position {
				line: *line,
				character: *character,
			};
			let offset_to_position = Position::from_byte_index_in_string(string, *offset);
			assert_eq!(position, offset_to_position);

			let position_to_offset = position.to_byte_index_in_string(string);
			assert_eq!(*offset, position_to_offset);
		}
	}

	#[test]
	fn position_past_end() {
		assert_eq!(
			Position::past_end("foo"),
			Position {
				line: 1,
				character: 0
			}
		);
		assert_eq!(
			Position::past_end("foo\nbar"),
			Position {
				line: 2,
				character: 0
			}
		);
		assert_eq!(
			Position::past_end("foo\nbar\nbaz\n"),
			Position {
				line: 4,
				character: 0
			}
		);
	}
}
