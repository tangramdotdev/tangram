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
	pub fn from_byte_index_in_string(string: &str, index: usize) -> Self {
		Self::try_from_byte_index_in_string(string, index).unwrap_or_else(|| {
			panic!(
				"string index {index} is out of bounds of string with length {}",
				string.len()
			);
		})
	}

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

	#[must_use]
	pub fn to_byte_index_in_string(self, string: &str) -> usize {
		self.try_to_byte_index_in_string(string).unwrap()
	}

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
