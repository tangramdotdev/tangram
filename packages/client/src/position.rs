use lsp_types as lsp;

/// A position in a string.
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
pub struct Position {
	#[tangram_serialize(id = 0)]
	pub line: u32,
	#[tangram_serialize(id = 1)]
	pub character: u32,
}

impl Position {
	#[must_use]
	pub fn try_from_byte_index_in_string(string: &str, index: usize) -> Option<Self> {
		// Check if the index is valid.
		if index > string.len() {
			return None;
		}

		// Count newlines before the index to get the line number.
		let line: u32 = string[..index]
			.chars()
			.filter(|&c| c == '\n')
			.count()
			.try_into()
			.ok()?;

		// Find the byte offset from the last newline to get the character position.
		let character: u32 = string[..index]
			.chars()
			.rev()
			.take_while(|&c| c != '\n')
			.map(char::len_utf8)
			.sum::<usize>()
			.try_into()
			.ok()?;

		let position = Self { line, character };

		Some(position)
	}

	#[must_use]
	pub fn try_to_byte_index_in_string(self, string: &str) -> Option<usize> {
		let mut current_line = 0;
		let mut line_start_index = 0;

		for (index, c) in string.char_indices() {
			// Check if we've reached the target line.
			if current_line == self.line {
				// Check if we've advanced enough bytes on this line.
				let bytes_from_line_start = index - line_start_index;
				if bytes_from_line_start >= self.character as usize {
					return Some(line_start_index + self.character as usize);
				}
			}

			// Move to next line if we encounter a newline.
			if c == '\n' {
				current_line += 1;
				line_start_index = index + c.len_utf8();
			}
		}

		// Check if position is at the end of the target line.
		if current_line == self.line {
			let bytes_from_line_start = string.len() - line_start_index;
			if bytes_from_line_start >= self.character as usize {
				return Some(line_start_index + self.character as usize);
			}
		}

		None
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
