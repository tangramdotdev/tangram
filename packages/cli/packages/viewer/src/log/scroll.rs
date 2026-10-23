use {
	super::Chunk,
	num::ToPrimitive as _,
	ratatui as tui,
	tangram_client::prelude::*,
	unicode_segmentation::{GraphemeCursor, GraphemeIncomplete},
	unicode_width::UnicodeWidthStr,
};

/// The visible range of a process log.
#[derive(Clone, Debug)]
pub struct Scroll {
	/// A buffer used for grapheme segmentation.
	buffer: Vec<u8>,

	/// The end position of the log text.
	end: u64,

	/// The area of the log view.
	pub(super) rect: tui::layout::Rect,

	/// The start position of the log text.
	start: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum Error {
	Append,
	Prepend,
}

pub struct Lines {
	pub content: Vec<String>,
	pub end: u64,
	pub start: u64,
}

// Track the grapheme parser state.
struct GraphemeParserState<'a, 'b> {
	buffer: &'b mut Vec<u8>,
	byte: usize,
	chunk: usize,
	chunks: &'a [Chunk],
	forwards: bool,
	start: usize,
}

impl Scroll {
	pub fn new(rect: tui::layout::Rect, chunks: &[Chunk]) -> tg::Result<Self, Error> {
		let mut buffer = Vec::with_capacity(64);
		let Some(chunk) = chunks.last() else {
			return Err(Error::Append);
		};
		let end = chunk.position + chunk.bytes.len().to_u64().unwrap();
		let (_, start) = Self::scroll_up_inner(
			&mut buffer,
			end,
			rect.width.to_usize().unwrap(),
			rect.height.to_usize().unwrap(),
			chunks,
		)?;

		Ok(Self {
			buffer,
			end,
			rect,
			start,
		})
	}

	pub fn scroll(&mut self, height: isize, chunks: &[Chunk]) -> tg::Result<usize, Error> {
		match height.cmp(&0) {
			std::cmp::Ordering::Equal => Ok(0),
			std::cmp::Ordering::Greater => self.scroll_down(height.unsigned_abs(), chunks),
			std::cmp::Ordering::Less => self.scroll_up(height.unsigned_abs(), chunks),
		}
	}

	fn scroll_down(&mut self, height: usize, chunks: &[Chunk]) -> tg::Result<usize, Error> {
		let (count, new_end) = Self::scroll_down_inner(
			&mut self.buffer,
			self.end,
			self.rect.width.to_usize().unwrap(),
			height,
			chunks,
		)?;
		if new_end != self.end {
			let (_, new_start) = Self::scroll_down_inner(
				&mut self.buffer,
				self.start,
				self.rect.width.to_usize().unwrap(),
				count,
				chunks,
			)?;
			self.start = new_start;
		}
		self.end = new_end;

		Ok(count)
	}

	fn scroll_up(&mut self, height: usize, chunks: &[Chunk]) -> tg::Result<usize, Error> {
		let (count, new_start) = Self::scroll_up_inner(
			&mut self.buffer,
			self.start,
			self.rect.width.to_usize().unwrap(),
			height,
			chunks,
		)?;
		if new_start != self.start {
			let (_, new_end) = Self::scroll_up_inner(
				&mut self.buffer,
				self.end,
				self.rect.width.to_usize().unwrap(),
				count,
				chunks,
			)?;
			self.end = new_end;
		}
		self.start = new_start;

		Ok(count)
	}

	pub fn read_lines(&mut self, chunks: &[Chunk]) -> tg::Result<Lines, Error> {
		Self::read_lines_inner(
			&mut self.buffer,
			self.start,
			self.rect.width.to_usize().unwrap(),
			self.rect.height.to_usize().unwrap(),
			chunks,
		)
	}

	fn scroll_up_inner(
		buffer: &mut Vec<u8>,
		mut position: u64,
		max_width: usize,
		num_lines: usize,
		chunks: &[Chunk],
	) -> tg::Result<(usize, u64), Error> {
		for count in 0..num_lines {
			let mut width = 0;
			loop {
				if position == 0 {
					return Ok((count, position));
				}
				let (grapheme, next_position) =
					Self::next_grapheme(buffer, position, false, chunks)?;
				let is_newline = grapheme.starts_with('\n');
				let grapheme_width = if grapheme == "\n" || grapheme == "\r\n" {
					0
				} else {
					grapheme.width()
				};
				width += grapheme_width;
				if width > max_width {
					break;
				}
				if !is_newline || width == 0 {
					position = next_position;
				} else if is_newline {
					break;
				}
			}
		}

		Ok((num_lines, position))
	}

	fn scroll_down_inner(
		buffer: &mut Vec<u8>,
		mut position: u64,
		max_width: usize,
		num_lines: usize,
		chunks: &[Chunk],
	) -> tg::Result<(usize, u64), Error> {
		let last = chunks.last().unwrap();
		let last = last.position + last.bytes.len().to_u64().unwrap();
		for count in 0..num_lines {
			let mut width = 0;
			loop {
				if position == last {
					return Ok((count, position));
				}
				let (grapheme, next_position) =
					Self::next_grapheme(buffer, position, true, chunks)?;
				let is_newline = grapheme.ends_with('\n');
				let grapheme_width = if grapheme == "\n" || grapheme == "\r\n" {
					0
				} else {
					grapheme.width()
				};
				width += grapheme_width;
				if width > max_width {
					break;
				}
				position = next_position;
				if is_newline {
					break;
				}
			}
		}

		Ok((num_lines, position))
	}

	fn read_lines_inner(
		buffer: &mut Vec<u8>,
		position: u64,
		max_width: usize,
		num_lines: usize,
		chunks: &[Chunk],
	) -> tg::Result<Lines, Error> {
		let mut lines = Lines {
			content: Vec::with_capacity(num_lines),
			end: position,
			start: position,
		};
		let last = chunks.last().unwrap();
		let last = last.position + last.bytes.len().to_u64().unwrap();
		'outer: for _ in 0..num_lines {
			let mut line = String::with_capacity(max_width);
			let mut width = 0;
			loop {
				if lines.end == last {
					lines
						.content
						.push(line.replace('\n', "").replace('\t', "  "));
					break 'outer;
				}
				let (grapheme, next_position) =
					Self::next_grapheme(buffer, lines.end, true, chunks)?;
				let is_newline = grapheme.ends_with('\n');
				let grapheme_width = if grapheme == "\n" || grapheme == "\r\n" {
					0
				} else {
					grapheme.width()
				};
				width += grapheme_width;
				if width > max_width {
					break;
				}
				line.push_str(grapheme);
				lines.end = next_position;
				if is_newline {
					break;
				}
			}
			lines
				.content
				.push(line.replace('\n', "").replace('\t', "  "));
		}

		Ok(lines)
	}

	// Advance one grapheme and return its next position.
	fn next_grapheme<'a>(
		buffer: &'a mut Vec<u8>,
		position: u64,
		forwards: bool,
		chunks: &[Chunk],
	) -> tg::Result<(&'a str, u64), Error> {
		let end_position = {
			let last = chunks.last().unwrap();
			last.position + last.bytes.len().to_u64().unwrap()
		};

		let (chunk, byte) = match position.cmp(&end_position) {
			std::cmp::Ordering::Equal => (chunks.len(), 0),
			std::cmp::Ordering::Greater => {
				return Err(Error::Append);
			},
			std::cmp::Ordering::Less => {
				let chunk = chunks
					.iter()
					.enumerate()
					.find_map(|(index, chunk)| {
						(chunk.position <= position
							&& chunk.position + chunk.bytes.len().to_u64().unwrap() > position)
							.then_some(index)
					})
					.ok_or(Error::Append)?;
				let byte = (position - chunks[chunk].position).to_usize().unwrap();
				(chunk, byte)
			},
		};

		let start = position.to_usize().unwrap();
		let mut grapheme_buffer = GraphemeParserState {
			buffer,
			byte,
			chunk,
			chunks,
			forwards,
			start,
		};

		let valid = grapheme_buffer.try_parse_grapheme()?;
		let size = grapheme_buffer.buffer.len().to_u64().unwrap();
		let position = if forwards {
			position + size
		} else {
			position - size
		};
		if valid {
			let grapheme = std::str::from_utf8(buffer).unwrap_or("\u{FFFD}");

			Ok((grapheme, position))
		} else {
			Ok(("\u{FFFD}", position))
		}
	}
}

impl GraphemeParserState<'_, '_> {
	// Parse codepoints until the Unicode segmentation cursor finds a grapheme boundary.
	fn try_parse_grapheme(&mut self) -> tg::Result<bool, Error> {
		// Reset the buffer and create a new grapheme cursor.
		self.buffer.clear();
		let length = {
			let last = self.chunks.last().unwrap();
			last.position.to_usize().unwrap() + last.bytes.len()
		};
		let mut cursor = GraphemeCursor::new(self.start, length, true);

		loop {
			// Try to scan the next codepoint.
			let (is_valid_utf8, num_bytes) = self.try_scan()?;
			if !is_valid_utf8 {
				// Retain one invalid sequence so the caller can render a Unicode replacement character.
				if self.buffer.is_empty() {
					self.commit_to_buffer(num_bytes);
					return Ok(false);
				}
				break;
			}

			// Add the codepoint bytes.
			let was_empty = self.buffer.is_empty();
			self.commit_to_buffer(num_bytes);

			// Reject structurally valid but non-canonical UTF-8.
			let Ok(text) = std::str::from_utf8(self.buffer) else {
				if was_empty {
					return Ok(false);
				}
				if self.forwards {
					self.buffer.truncate(self.buffer.len() - num_bytes);
				} else {
					self.buffer.drain(..num_bytes);
				}
				break;
			};

			// Find the next grapheme boundary.
			let result = if self.forwards {
				cursor.next_boundary(text, self.start)
			} else {
				cursor.prev_boundary(text, self.start)
			};

			match result {
				// Continue with another codepoint.
				Err(GraphemeIncomplete::NextChunk | GraphemeIncomplete::PrevChunk) => (),

				// Provide the cursor with preceding context.
				Err(GraphemeIncomplete::PreContext(end)) => {
					let Some((context, start)) = self.try_pre_context(end) else {
						return Err(Error::Prepend);
					};
					cursor.provide_context(context, start);
				},

				Err(_) => unreachable!(),
				// Finish at the end of the stream.
				Ok(None) => break,

				// Remove the codepoint beyond the grapheme boundary.
				Ok(Some(boundary)) => {
					if self.forwards {
						let end = self.start + self.buffer.len();
						for _ in boundary..end {
							self.buffer.pop();
						}
					} else {
						for _ in self.start..boundary {
							self.buffer.remove(0);
						}
					}
					break;
				},
			}
		}

		Ok(true)
	}

	// Read one codepoint and return its validity and byte length.
	fn try_scan(&self) -> tg::Result<(bool, usize), Error> {
		if self.forwards {
			self.try_scan_forward()
		} else {
			self.try_scan_reverse()
		}
	}

	fn try_scan_forward(&self) -> tg::Result<(bool, usize), Error> {
		let current = self.chunks[self.chunk].bytes[self.byte];
		let num_bytes = if current & 0b1111_0000 == 0b1111_0000 {
			3
		} else if current & 0b1110_0000 == 0b1110_0000 {
			2
		} else if current & 0b1100_0000 == 0b1100_0000 {
			1
		} else if current & 0b1000_0000 == 0b0000_0000 {
			0
		} else {
			return Ok((false, 1));
		};

		let mut chunk = self.chunk;
		let mut byte = self.byte;
		for continuation_index in 0..num_bytes {
			let (next_chunk, next_byte) = if byte == self.chunks[chunk].bytes.len() - 1 {
				(chunk + 1, 0)
			} else {
				(chunk, byte + 1)
			};
			if next_chunk >= self.chunks.len() || next_byte >= self.chunks[next_chunk].bytes.len() {
				return Err(Error::Append);
			}
			let continuation = self.chunks[next_chunk].bytes[next_byte];
			if continuation & 0b1100_0000 != 0b1000_0000 {
				return Ok((false, 1 + continuation_index));
			}
			chunk = next_chunk;
			byte = next_byte;
		}

		Ok((true, num_bytes + 1))
	}

	fn try_scan_reverse(&self) -> tg::Result<(bool, usize), Error> {
		let mut num_bytes = 0;
		let mut chunk = self.chunk;
		let mut byte = self.byte;
		let is_valid_utf8 = loop {
			if chunk == 0 && byte == 0 {
				return Err(Error::Prepend);
			} else if byte == 0 {
				chunk -= 1;
				byte = self.chunks[chunk].bytes.len() - 1;
			} else {
				byte -= 1;
			}
			let candidate = self.chunks[chunk].bytes[byte];
			if candidate & 0b1111_0000 == 0b1111_0000 {
				break num_bytes == 3;
			} else if candidate & 0b1110_0000 == 0b1110_0000 {
				break num_bytes == 2;
			} else if candidate & 0b1100_0000 == 0b1100_0000 {
				break num_bytes == 1;
			} else if candidate & 0b1000_0000 == 0 {
				break num_bytes == 0;
			} else if candidate & 0b1100_0000 != 0b1000_0000 {
				break false;
			}

			num_bytes += 1;
		};
		if is_valid_utf8 || num_bytes == 0 {
			num_bytes += 1;
		}
		Ok((is_valid_utf8, num_bytes))
	}

	// Append or prepend `num_bytes` to the buffer from the chunk stream.
	fn commit_to_buffer(&mut self, num_bytes: usize) {
		for _ in 0..num_bytes {
			if self.forwards {
				self.buffer.push(self.chunks[self.chunk].bytes[self.byte]);
				if self.byte == self.chunks[self.chunk].bytes.len() - 1 {
					self.chunk += 1;
					self.byte = 0;
				} else {
					self.byte += 1;
				}
			} else {
				if self.byte == 0 {
					self.chunk -= 1;
					self.byte = self.chunks[self.chunk].bytes.len() - 1;
				} else {
					self.byte -= 1;
				}
				self.start = self.start.saturating_sub(1);
				self.buffer
					.insert(0, self.chunks[self.chunk].bytes[self.byte]);
			}
		}
	}

	// Find the preceding context required by the segmentation cursor.
	fn try_pre_context(&self, end: usize) -> Option<(&str, usize)> {
		let chunk = self.chunks[..=self.chunk]
			.iter()
			.rev()
			.find(|chunk| chunk.position.to_usize().unwrap() < end)?;
		let end_byte = end - chunk.position.to_usize().unwrap();
		for start_byte in 0..chunk.bytes.len() {
			let bytes = &chunk.bytes[start_byte..end_byte];
			if let Ok(string) = std::str::from_utf8(bytes) {
				return Some((string, chunk.position.to_usize().unwrap() + start_byte));
			}
		}

		None
	}
}

#[cfg(test)]
mod tests {
	use {
		super::{Chunk, Error, Scroll},
		num::ToPrimitive as _,
		ratatui::layout::Rect,
		tangram_client::prelude::*,
	};

	// Constructing a scroll without buffered chunks requests an append.
	#[test]
	fn new_requests_append_for_empty_chunks() {
		let result = Scroll::new(Rect::new(0, 0, 20, 1), &[]);
		assert!(matches!(result, Err(Error::Append)));
	}

	// Scrolling up by one line and then back down by one line returns to the original start, end, and rendered lines.
	#[test]
	fn scroll_up_and_down() {
		let chunks = [Chunk {
			bytes: b"1 abcdef\n2 abcdef\n3 abcdef\n".to_vec().into(),
			position: 0,
			stream: tg::process::stdio::Stream::Stdout,
		}];
		let mut scroll = Scroll::new(Rect::new(0, 0, 20, 1), &chunks).unwrap();
		let (initial_start, initial_end) = (scroll.start, scroll.end);
		let initial_lines = scroll.read_lines(&chunks).unwrap();

		scroll.scroll_up(1, &chunks).unwrap();
		scroll.scroll_down(1, &chunks).unwrap();
		let (start, end) = (scroll.start, scroll.end);
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(initial_start, start);
		assert_eq!(initial_end, end);
		assert_eq!(initial_lines.content, lines.content);
	}

	// An invalid UTF-8 byte between valid chunks is decoded as the replacement character when stepping graphemes forward and backward.
	#[test]
	fn replaces_invalid_utf8() {
		let chunks = vec![
			Chunk {
				bytes: b"a".to_vec().into(),
				position: 0,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: vec![0b1010_1010].into(),
				position: 1,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: b"b".to_vec().into(),
				position: 2,
				stream: tg::process::stdio::Stream::Stdout,
			},
		];
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let mut position = 0;
		let (grapheme, new_position) =
			Scroll::next_grapheme(buffer, position, true, &chunks).unwrap();
		assert_eq!(grapheme, "a");
		position = new_position;
		let (grapheme, new_position) =
			Scroll::next_grapheme(buffer, position, true, &chunks).unwrap();
		assert_eq!(grapheme, "\u{FFFD}");
		position = new_position;
		let (grapheme, new_position) =
			Scroll::next_grapheme(buffer, position, true, &chunks).unwrap();
		assert_eq!(grapheme, "b");
		position = new_position;
		let (grapheme, new_position) =
			Scroll::next_grapheme(buffer, position, false, &chunks).unwrap();
		assert_eq!(grapheme, "b");
		position = new_position;
		let (grapheme, new_position) =
			Scroll::next_grapheme(buffer, position, false, &chunks).unwrap();
		assert_eq!(grapheme, "\u{FFFD}");
		position = new_position;
		let (grapheme, _) = Scroll::next_grapheme(buffer, position, false, &chunks).unwrap();
		assert_eq!(grapheme, "a");
	}

	// A non-canonical UTF-8 sequence renders as one replacement character without panicking.
	#[test]
	fn replaces_noncanonical_utf8() {
		let chunks = [Chunk {
			bytes: vec![0xc0, 0x80].into(),
			position: 0,
			stream: tg::process::stdio::Stream::Stdout,
		}];
		let mut buffer = Vec::new();
		let (grapheme, position) = Scroll::next_grapheme(&mut buffer, 0, true, &chunks).unwrap();
		assert_eq!(grapheme, "\u{FFFD}");
		assert_eq!(position, 2);
		let (grapheme, position) =
			Scroll::next_grapheme(&mut buffer, position, false, &chunks).unwrap();
		assert_eq!(grapheme, "\u{FFFD}");
		assert_eq!(position, 0);
	}

	// An incomplete UTF-8 codepoint at the end of the available chunks requests another chunk instead of indexing past the chunk list.
	#[test]
	fn requests_append_for_incomplete_utf8() {
		let chunks = vec![Chunk {
			bytes: vec![b'a', 0xf0].into(),
			position: 0,
			stream: tg::process::stdio::Stream::Stdout,
		}];
		let mut buffer = Vec::new();
		let result = Scroll::next_grapheme(&mut buffer, 1, true, &chunks);
		assert!(matches!(result, Err(Error::Append)));
	}

	// A UTF-8 codepoint split across chunks is decoded as one grapheme.
	#[test]
	fn decodes_split_utf8() {
		let chunks = vec![
			Chunk {
				bytes: vec![0xf0, 0x9f].into(),
				position: 0,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: vec![0x98, 0x80].into(),
				position: 2,
				stream: tg::process::stdio::Stream::Stdout,
			},
		];
		let mut buffer = Vec::new();
		let (grapheme, position) = Scroll::next_grapheme(&mut buffer, 0, true, &chunks).unwrap();
		assert_eq!(grapheme, "😀");
		assert_eq!(position, 4);
	}

	// Multi-byte emoji graphemes spanning chunks render into the expected lines for both non-trailing and trailing newline cases.
	#[test]
	fn renders_emoji() {
		// Test the non-tailing case.
		let chunks = vec![
			Chunk {
				bytes: "1——👍👌👉👈——\n".as_bytes().to_vec().into(),
				position: 0,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: "2——👍👌👉👈——\n".as_bytes().to_vec().into(),
				position: 30,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: "3——👍👌👉👈——\n".as_bytes().to_vec().into(),
				position: 60,
				stream: tg::process::stdio::Stream::Stdout,
			},
		];
		let mut scroll = Scroll {
			buffer: Vec::new(),
			end: chunks.last().unwrap().position
				+ chunks.last().unwrap().bytes.len().to_u64().unwrap(),
			rect: Rect::new(0, 0, 20, 3),
			start: 0,
		};
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(
			&lines.content,
			&["1——👍👌👉👈——", "2——👍👌👉👈——", "3——👍👌👉👈——"]
		);

		// Test the tailing case.
		let chunks = vec![
			Chunk {
				bytes:
					"\"0——👍👌👉👈——\"\n\"1——👍👌👉👈——\"\n\"2——👍👌👉👈——\"\n\"3——👍👌👉👈——\"\n"
						.as_bytes()
						.to_vec()
						.into(),
				position: 0,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: "\"4——👍👌👉👈——\"\n".as_bytes().to_vec().into(),
				position: 128,
				stream: tg::process::stdio::Stream::Stdout,
			},
		];
		let mut scroll = Scroll::new(Rect::new(0, 0, 20, 10), &chunks).unwrap();
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(
			&lines.content,
			&[
				"\"0——👍👌👉👈——\"",
				"\"1——👍👌👉👈——\"",
				"\"2——👍👌👉👈——\"",
				"\"3——👍👌👉👈——\"",
				"\"4——👍👌👉👈——\"",
				"",
			]
		);
	}

	// Emoji wider than the view wrap onto separate lines at the width boundary.
	#[test]
	fn wraps_emoji() {
		let chunks = vec![Chunk {
			bytes: "😀😀".as_bytes().to_vec().into(),
			position: 0,
			stream: tg::process::stdio::Stream::Stdout,
		}];
		let mut scroll = Scroll::new(Rect::new(0, 0, 2, 4), &chunks).unwrap();
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(&lines.content[0], "😀");
		assert_eq!(&lines.content[1], "😀");
	}

	// Reading lines from a freshly tailed scroll produces one line per chunk plus the trailing empty line.
	#[test]
	fn renders_tailing_chunks() {
		let mut position = 0;
		let mut chunks = Vec::new();
		for index in 0..24 {
			let bytes = format!("\"log line {index}\"\n").into();
			let chunk = Chunk {
				bytes,
				position,
				stream: tg::process::stdio::Stream::Stdout,
			};
			position += chunk.bytes.len().to_u64().unwrap();
			chunks.push(chunk);
		}
		let mut scroll = Scroll::new(Rect::new(0, 0, 80, 40), &chunks).unwrap();
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(lines.content.len(), chunks.len() + 1);
	}

	// Scrolling up moves the position toward the start and clamps at zero once the top is reached.
	#[test]
	fn scroll_up() {
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let max_width = 80;
		let mut position = 0;
		let mut chunks = Vec::new();
		for index in 0..8 {
			let bytes = format!("\"log line {index}\"\n").into();
			let chunk = Chunk {
				bytes,
				position,
				stream: tg::process::stdio::Stream::Stdout,
			};
			position += chunk.bytes.len().to_u64().unwrap();
			chunks.push(chunk);
		}

		let (_, new_position) =
			Scroll::scroll_up_inner(buffer, position, max_width, chunks.len() - 1, &chunks)
				.unwrap();
		assert_eq!(new_position, chunks[1].position);
		position = new_position;

		let (_, new_position) =
			Scroll::scroll_up_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(new_position, 0);
		position = new_position;

		let (_, new_position) =
			Scroll::scroll_up_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(new_position, 0);
	}

	// Scrolling down moves the position toward the end and clamps at the end once it is reached.
	#[test]
	fn scroll_down() {
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let max_width = 80;
		let mut position = 0;
		let mut chunks = Vec::new();
		for index in 0..8 {
			let bytes = format!("\"log line {index}\"\n").into();
			let chunk = Chunk {
				bytes,
				position,
				stream: tg::process::stdio::Stream::Stdout,
			};
			position += chunk.bytes.len().to_u64().unwrap();
			chunks.push(chunk);
		}

		let mut position = 0;
		let (_, new_position) =
			Scroll::scroll_down_inner(buffer, position, max_width, chunks.len() - 1, &chunks)
				.unwrap();
		assert_eq!(new_position, chunks[7].position);
		position = new_position;

		let (_, new_position) =
			Scroll::scroll_down_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(
			new_position,
			chunks.last().unwrap().position + chunks.last().unwrap().bytes.len().to_u64().unwrap()
		);
		position = new_position;

		let (_, new_position) =
			Scroll::scroll_down_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(
			new_position,
			chunks.last().unwrap().position + chunks.last().unwrap().bytes.len().to_u64().unwrap()
		);
	}

	// Scrolling up past the beginning of the available chunks reports a prepend error.
	#[test]
	fn scroll_up_requests_prepend() {
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let max_width = 80;
		let num_lines = 26;

		let chunks = [
			Chunk {
				bytes: b"\"doing stuff 6...\"\n".to_vec().into(),
				position: 114,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: b"\"doing stuff 7...\"\n".to_vec().into(),
				position: 133,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: b"\"doing stuff 8...\"\n".to_vec().into(),
				position: 152,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: b"\"doing stuff 9...\"\n".to_vec().into(),
				position: 171,
				stream: tg::process::stdio::Stream::Stdout,
			},
			Chunk {
				bytes: b"\"doing stuff 10...\"\n".to_vec().into(),
				position: 190,
				stream: tg::process::stdio::Stream::Stdout,
			},
		];

		let position =
			chunks.last().unwrap().position + chunks.last().unwrap().bytes.len().to_u64().unwrap();
		let result = Scroll::scroll_up_inner(buffer, position, max_width, num_lines, &chunks);
		assert!(matches!(result, Err(Error::Prepend)));
	}
}
