use {
	num::ToPrimitive as _,
	ratatui as tui,
	tangram_client::prelude::*,
	unicode_segmentation::{GraphemeCursor, GraphemeIncomplete},
	unicode_width::UnicodeWidthStr,
};

/// Represents the current state of log's scroll, between some `start` and `end` positions.
#[derive(Clone, Debug)]
pub struct Scroll {
	/// The area of the log view.
	pub(super) rect: tui::layout::Rect,

	/// The start position of the log text.
	start: u64,

	/// The end position of the log text.
	end: u64,

	/// A buffer used for grapheme segementation.
	buffer: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub enum Error {
	Append,
	Prepend,
}

// Helper struct that represent the state of the grapheme parser.
struct GraphemeParserState<'a, 'b> {
	buffer: &'b mut Vec<u8>,
	byte: usize,
	chunk: usize,
	chunks: &'a [tg::process::log::get::Chunk],
	forward: bool,
	start: usize,
}

impl Scroll {
	pub fn new(
		rect: tui::layout::Rect,
		chunks: &[tg::process::log::get::Chunk],
	) -> tg::Result<Self, Error> {
		let mut buffer = Vec::with_capacity(64);
		let chunk = chunks.last().unwrap();
		let end = chunk.position + chunk.bytes.len().to_u64().unwrap();
		let (_, start) = scroll_up_inner(
			&mut buffer,
			end,
			rect.width.to_usize().unwrap(),
			rect.height.to_usize().unwrap(),
			chunks,
		)?;
		Ok(Self {
			rect,
			start,
			end,
			buffer,
		})
	}

	pub fn scroll_up(
		&mut self,
		height: usize,
		chunks: &[tg::process::log::get::Chunk],
	) -> tg::Result<usize, Error> {
		let (count, new_start) = scroll_up_inner(
			&mut self.buffer,
			self.start,
			self.rect.width.to_usize().unwrap(),
			height,
			chunks,
		)?;
		if new_start != self.start {
			let (_, new_end) = scroll_up_inner(
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

	pub fn scroll_down(
		&mut self,
		height: usize,
		chunks: &[tg::process::log::get::Chunk],
	) -> tg::Result<usize, Error> {
		let (count, new_end) = scroll_down_inner(
			&mut self.buffer,
			self.end,
			self.rect.width.to_usize().unwrap(),
			height,
			chunks,
		)?;
		if new_end != self.end {
			let (_, new_start) = scroll_down_inner(
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

	pub fn read_lines(
		&mut self,
		chunks: &[tg::process::log::get::Chunk],
	) -> tg::Result<Vec<String>, Error> {
		read_lines_inner(
			&mut self.buffer,
			self.start,
			self.rect.width.to_usize().unwrap(),
			self.rect.height.to_usize().unwrap(),
			chunks,
		)
	}
}

fn scroll_up_inner(
	buffer: &mut Vec<u8>,
	mut position: u64,
	max_width: usize,
	num_lines: usize,
	chunks: &[tg::process::log::get::Chunk],
) -> tg::Result<(usize, u64), Error> {
	for count in 0..num_lines {
		let mut width = 0;
		loop {
			if position == 0 {
				return Ok((count, position));
			}
			let (grapheme, next_position) = next_grapheme(buffer, position, false, chunks)?;
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
	chunks: &[tg::process::log::get::Chunk],
) -> tg::Result<(usize, u64), Error> {
	let last = chunks.last().unwrap();
	let last = last.position + last.bytes.len().to_u64().unwrap();
	for count in 0..num_lines {
		let mut width = 0;
		loop {
			if position == last {
				return Ok((count, position));
			}
			let (grapheme, next_position) = next_grapheme(buffer, position, true, chunks)?;
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
	mut position: u64,
	max_width: usize,
	num_lines: usize,
	chunks: &[tg::process::log::get::Chunk],
) -> tg::Result<Vec<String>, Error> {
	let last = chunks.last().unwrap();
	let last = last.position + last.bytes.len().to_u64().unwrap();
	let mut lines = Vec::with_capacity(num_lines);
	'outer: for _ in 0..num_lines {
		let mut line = String::with_capacity(max_width);
		let mut width = 0;
		loop {
			if position == last {
				lines.push(line.replace('\n', "").replace('\t', "  "));
				break 'outer;
			}
			let (grapheme, next_position) = next_grapheme(buffer, position, true, chunks)?;
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
			position = next_position;
			if is_newline {
				break;
			}
		}
		lines.push(line.replace('\n', "").replace('\t', "  "));
	}
	Ok(lines)
}

// Advance the scroll position by one grapheme forward or backward, and return the grapheme and its end position.
fn next_grapheme<'a>(
	buffer: &'a mut Vec<u8>,
	position: u64,
	forward: bool,
	chunks: &[tg::process::log::get::Chunk],
) -> tg::Result<(&'a str, u64), Error> {
	let end_position = {
		let last = chunks.last().unwrap();
		last.position + last.bytes.len().to_u64().unwrap()
	};

	let (chunk, byte) = match position.cmp(&end_position) {
		std::cmp::Ordering::Greater => {
			return Err(Error::Append);
		},
		std::cmp::Ordering::Equal => (chunks.len(), 0),
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
		forward,
		start,
	};

	let is_valid_utf8 = grapheme_buffer.try_parse_grapheme()?;
	let size = grapheme_buffer.buffer.len().to_u64().unwrap();
	let position = if forward {
		position + size
	} else {
		position - size
	};
	if is_valid_utf8 {
		let grapheme = std::str::from_utf8(buffer).unwrap();
		Ok((grapheme, position))
	} else {
		Ok(("\u{FFFD}", position))
	}
}

impl GraphemeParserState<'_, '_> {
	// The grapheme parsing algorithm is as folows:
	//
	//	- Attempt to scan one codepoint forward or backward in the chunk stream.
	// 		- If we reach either end or beginning of the stream, emit an error to append or prepend
	//	- If we cannot parse a codepoint because ict is not valid utf-8, bail out early.
	//	- Try to find the next grapheme boundary based on the codepoints in the buffer
	//	- If it is Ok(None), break early because we've reached the start/end of the stream
	// 	- If it is Ok(Some(boundary)), remove (buffer.len() - boundary) bytes from the front or back of the buffer as necessary.
	//	- Else, continue adding codepoints.
	fn try_parse_grapheme(&mut self) -> tg::Result<bool, Error> {
		// Reset the buffer and create a new grapheme cursor.
		self.buffer.clear();
		let len = {
			let last = self.chunks.last().unwrap();
			last.position.to_usize().unwrap() + last.bytes.len()
		};
		let mut cursor = GraphemeCursor::new(self.start, len, true);

		loop {
			// Try to scan the next codepoint.
			let (is_valid_utf8, num_bytes) = self.try_scan()?;
			if !is_valid_utf8 {
				// If we have invalid UTF-8 and the buffer is empty, the caller must replace with a unicode replacement character.
				if self.buffer.is_empty() {
					self.commit_to_buffer(num_bytes);
					return Ok(false);
				}
				break;
			}

			// Add the codepoint's bytes to the buffer.
			self.commit_to_buffer(num_bytes);

			// Try to find the next grapheme boundary.
			let text = std::str::from_utf8(self.buffer).unwrap();
			let result = if self.forward {
				cursor.next_boundary(text, self.start)
			} else {
				cursor.prev_boundary(text, self.start)
			};

			match result {
				// End of stream, done.
				Ok(None) => break,

				// There is a complete grapheme in the buffer, remove the last codepoint added.
				Ok(Some(boundary)) => {
					if self.forward {
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

				// We need more codepoints, continue parsing.
				Err(GraphemeIncomplete::NextChunk | GraphemeIncomplete::PrevChunk) => (),

				// There's not enough information in the buffer, grab some precontext.
				Err(GraphemeIncomplete::PreContext(end)) => {
					let Some((context, start)) = self.get_pre_context(end) else {
						return Err(Error::Prepend);
					};
					cursor.provide_context(context, start);
				},

				Err(_) => unreachable!(),
			}
		}

		Ok(true)
	}

	// Append or prepend `num_bytes` to the buffer from the chunk stream.
	fn commit_to_buffer(&mut self, num_bytes: usize) {
		for _ in 0..num_bytes {
			if self.forward {
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

	// In the case that there's not enough information to determine where the boundary is, we need to provide "pre context" to the cursor.
	fn get_pre_context(&self, end: usize) -> Option<(&str, usize)> {
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

	// Read one codepoint forward or reverse, returning if it's valid and how many bytes long it is.
	fn try_scan(&self) -> tg::Result<(bool, usize), Error> {
		if self.forward {
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
		for n in 0..num_bytes {
			let (next_chunk, next_byte) = if chunk == self.chunks.len() {
				return Err(Error::Append);
			} else if byte == self.chunks[chunk].bytes.len() - 1 {
				(chunk + 1, 0)
			} else {
				(chunk, byte + 1)
			};
			let next = self.chunks[next_chunk].bytes[next_byte];
			if next & 0b1100_0000 != 0b1000_0000 {
				return Ok((false, 1 + n));
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
			let next = self.chunks[chunk].bytes[byte];
			if next & 0b1111_0000 == 0b1111_0000 {
				break num_bytes == 3;
			} else if next & 0b1110_0000 == 0b1110_0000 {
				break num_bytes == 2;
			} else if next & 0b1100_0000 == 0b1100_0000 {
				break num_bytes == 1;
			} else if next & 0b1000_0000 == 0 {
				break num_bytes == 0;
			} else if next & 0b1100_0000 != 0b1000_0000 {
				break false;
			}

			num_bytes += 1;
		};

		if is_valid_utf8 {
			num_bytes += 1;
		}
		Ok((is_valid_utf8, num_bytes))
	}
}

#[cfg(test)]
mod tests {
	use {
		super::{Error, Scroll, next_grapheme, scroll_down_inner, scroll_up_inner},
		num::ToPrimitive as _,
		ratatui::layout::Rect,
		tangram_client::prelude::*,
	};

	#[test]
	fn scroll_up_and_down() {
		let chunks = [tg::process::log::get::Chunk {
			position: 0,
			bytes: b"1 abcdef\n2 abcdef\n3 abcdef\n".to_vec().into(),
			stream: tangram_client::process::log::Stream::Stdout,
			timestamp: 0,
		}];
		let mut scroll = Scroll::new(Rect::new(0, 0, 20, 1), &chunks).unwrap();
		let (init_start, init_end) = (scroll.start, scroll.end);
		let init_lines = scroll.read_lines(&chunks).unwrap();

		scroll.scroll_up(1, &chunks).unwrap();
		scroll.scroll_down(1, &chunks).unwrap();
		let (start, end) = (scroll.start, scroll.end);
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(init_start, start);
		assert_eq!(init_end, end);
		assert_eq!(init_lines, lines);
	}

	#[test]
	fn invalid_utf8() {
		let chunks = vec![
			tg::process::log::get::Chunk {
				position: 0,
				bytes: b"a".to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 0,
			},
			tg::process::log::get::Chunk {
				position: 1,
				bytes: vec![0b1010_1010].into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 1,
			},
			tg::process::log::get::Chunk {
				position: 2,
				bytes: b"b".to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 2,
			},
		];
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let mut position = 0;
		let (ch, new_position) = next_grapheme(buffer, position, true, &chunks).unwrap();
		assert_eq!(ch, "a");
		position = new_position;
		let (ch, new_position) = next_grapheme(buffer, position, true, &chunks).unwrap();
		assert_eq!(ch, "\u{FFFD}");
		position = new_position;
		let (ch, new_position) = next_grapheme(buffer, position, true, &chunks).unwrap();
		assert_eq!(ch, "b");
		position = new_position;
		let (ch, new_position) = next_grapheme(buffer, position, false, &chunks).unwrap();
		assert_eq!(ch, "b");
		position = new_position;
		let (ch, new_position) = next_grapheme(buffer, position, false, &chunks).unwrap();
		assert_eq!(ch, "\u{FFFD}");
		position = new_position;
		let (ch, _) = next_grapheme(buffer, position, false, &chunks).unwrap();
		assert_eq!(ch, "a");
	}

	#[test]
	fn emoji() {
		// Non tailing case.
		let chunks = vec![
			tg::process::log::get::Chunk {
				position: 0,
				bytes: "1——👍👌👉👈——\n".as_bytes().to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 0,
			},
			tg::process::log::get::Chunk {
				position: 30,
				bytes: "2——👍👌👉👈——\n".as_bytes().to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 30,
			},
			tg::process::log::get::Chunk {
				position: 60,
				bytes: "3——👍👌👉👈——\n".as_bytes().to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 60,
			},
		];
		let mut scroll = Scroll {
			buffer: Vec::new(),
			rect: Rect::new(0, 0, 20, 3),
			start: 0,
			end: chunks.last().unwrap().position
				+ chunks.last().unwrap().bytes.len().to_u64().unwrap(),
		};
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(&lines, &["1——👍👌👉👈——", "2——👍👌👉👈——", "3——👍👌👉👈——"]);

		// Tailing case.
		let chunks = vec![
			tg::process::log::get::Chunk {
				position: 0,
				bytes:
					"\"0——👍👌👉👈——\"\n\"1——👍👌👉👈——\"\n\"2——👍👌👉👈——\"\n\"3——👍👌👉👈——\"\n"
						.as_bytes()
						.to_vec()
						.into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 0,
			},
			tg::process::log::get::Chunk {
				position: 128,
				bytes: "\"4——👍👌👉👈——\"\n".as_bytes().to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 1,
			},
		];
		let mut scroll = Scroll::new(Rect::new(0, 0, 20, 10), &chunks).unwrap();
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(
			&lines,
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

	#[test]
	fn word_wrap_emoji() {
		let chunks = vec![tg::process::log::get::Chunk {
			position: 0,
			bytes: "😀😀".as_bytes().to_vec().into(),
			stream: tangram_client::process::log::Stream::Stdout,
			timestamp: 0,
		}];
		let mut scroll = Scroll::new(Rect::new(0, 0, 2, 4), &chunks).unwrap();
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(&lines[0], "😀");
		assert_eq!(&lines[1], "😀");
	}

	#[test]
	fn tailing() {
		let mut position = 0;
		let mut chunks = Vec::new();
		for n in 0..24 {
			let bytes = format!("\"log line {n}\"\n").into();
			let chunk = tg::process::log::get::Chunk {
				position,
				bytes,
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: n,
			};
			position += chunk.bytes.len().to_u64().unwrap();
			chunks.push(chunk);
		}
		let mut scroll = Scroll::new(Rect::new(0, 0, 80, 40), &chunks).unwrap();
		let lines = scroll.read_lines(&chunks).unwrap();
		assert_eq!(lines.len(), chunks.len() + 1);
	}

	#[test]
	fn scroll_up() {
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let max_width = 80;
		let mut position = 0;
		let mut chunks = Vec::new();
		for n in 0..8 {
			let bytes = format!("\"log line {n}\"\n").into();
			let chunk = tg::process::log::get::Chunk {
				position,
				bytes,
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: n,
			};
			position += chunk.bytes.len().to_u64().unwrap();
			chunks.push(chunk);
		}

		let (_, new_position) =
			scroll_up_inner(buffer, position, max_width, chunks.len() - 1, &chunks).unwrap();
		assert_eq!(new_position, chunks[1].position);
		position = new_position;

		let (_, new_position) = scroll_up_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(new_position, 0);
		position = new_position;

		let (_, new_position) = scroll_up_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(new_position, 0);
	}

	#[test]
	fn scroll_down() {
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let max_width = 80;
		let mut position = 0;
		let mut chunks = Vec::new();
		for n in 0..8 {
			let bytes = format!("\"log line {n}\"\n").into();
			let chunk = tg::process::log::get::Chunk {
				position,
				bytes,
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 0,
			};
			position += chunk.bytes.len().to_u64().unwrap();
			chunks.push(chunk);
		}

		let mut position = 0;
		let (_, new_position) =
			scroll_down_inner(buffer, position, max_width, chunks.len() - 1, &chunks).unwrap();
		assert_eq!(new_position, chunks[7].position);
		position = new_position;

		let (_, new_position) =
			scroll_down_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(
			new_position,
			chunks.last().unwrap().position + chunks.last().unwrap().bytes.len().to_u64().unwrap()
		);
		position = new_position;

		let (_, new_position) =
			scroll_down_inner(buffer, position, max_width, 10, &chunks).unwrap();
		assert_eq!(
			new_position,
			chunks.last().unwrap().position + chunks.last().unwrap().bytes.len().to_u64().unwrap()
		);
	}

	#[test]
	fn incomplete() {
		let mut buffer = Vec::new();
		let buffer = &mut buffer;
		let max_width = 80;
		let num_lines = 26;

		let chunks = [
			tg::process::log::get::Chunk {
				position: 114,
				bytes: b"\"doing stuff 6...\"\n".to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 0,
			},
			tg::process::log::get::Chunk {
				position: 133,
				bytes: b"\"doing stuff 7...\"\n".to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 1,
			},
			tg::process::log::get::Chunk {
				position: 152,
				bytes: b"\"doing stuff 8...\"\n".to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 2,
			},
			tg::process::log::get::Chunk {
				position: 171,
				bytes: b"\"doing stuff 9...\"\n".to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 3,
			},
			tg::process::log::get::Chunk {
				position: 190,
				bytes: b"\"doing stuff 10...\"\n".to_vec().into(),
				stream: tangram_client::process::log::Stream::Stdout,
				timestamp: 4,
			},
		];

		let position =
			chunks.last().unwrap().position + chunks.last().unwrap().bytes.len().to_u64().unwrap();
		let result = scroll_up_inner(buffer, position, max_width, num_lines, &chunks);
		assert!(matches!(result, Err(Error::Prepend)));
	}
}
