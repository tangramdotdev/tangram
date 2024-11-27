#![allow(dead_code)]
use ansi_to_tui::IntoText;
use bytes::Bytes;
use layout::Position;
use num::ToPrimitive;
use ratatui::{
	prelude::*,
	widgets::{Paragraph, Wrap},
};
use regex::Regex;
use std::{
	str,
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tg::Either;

pub const ANSI_REGEX: &str =
	r"[\x1b\x9b]\[[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]";
pub const BORDER_WIDTH: usize = 2;
pub const COLUMN_WIDTH: usize = 36;
pub const POSITION_WIDTH: usize = 10;
pub const BYTES_PER_COLUMN: usize = 8;

type Content = Either<String, Bytes>;

pub struct Data<H> {
	handle: H,
	value: tg::Value,
	state: RwLock<State>,
}

struct State {
	area: Rect,
	content: Content,
	num_columns: usize,
	scroll: usize,
	line_count: usize,
}

impl<H> Data<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, value: &tg::Value, area: Rect) -> Arc<Self> {
		let handle = handle.clone();
		let value = value.clone();
		let state = RwLock::new(State {
			area,
			content: Either::Left(String::new()),
			num_columns: Self::calc_num_columns(area.width.to_usize().unwrap()),
			scroll: 0,
			line_count: 0,
		});
		let info = Arc::new(Self {
			handle,
			value,
			state,
		});
		tokio::task::spawn({
			let info = info.clone();
			async move {
				let content = info
					.get_content()
					.await
					.unwrap_or_else(|error| Either::Left(format!("error: {error}")));
				let mut state = info.state.write().unwrap();
				state.line_count = Self::calc_line_count(&content);
				state.content = content;
			}
		});
		info
	}

	fn calc_num_columns(width: usize) -> usize {
		(width
			.saturating_sub(BORDER_WIDTH)
			.saturating_sub(POSITION_WIDTH))
			/ COLUMN_WIDTH
	}

	fn calc_line_count(content: &Content) -> usize {
		match content {
			Either::Left(text) => text.lines().count(),
			Either::Right(bytes) => bytes.len() / BYTES_PER_COLUMN,
		}
	}

	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		let state = self.state.read().unwrap();
		match &state.content {
			Either::Left(text) => {
				let lines = text
					.lines()
					.skip(state.scroll)
					.map(|line| line.replace('\t', "  "))
					.map(Line::raw)
					.take(state.area.height.into())
					.collect::<Vec<_>>();
				Paragraph::new(lines)
					.wrap(Wrap { trim: false })
					.render(area, buf);
			},
			Either::Right(bytes) => {
				let (show_char, show_pos, num_columns) = if state.num_columns == 0 {
					(false, false, 1)
				} else {
					(true, true, state.num_columns)
				};
				let mut out = Vec::new();
				let mut printer = hexyl::PrinterBuilder::new(&mut out)
					.show_color(true)
					.show_char_panel(show_char)
					.show_position_panel(show_pos)
					.with_border_style(hexyl::BorderStyle::None)
					.enable_squeezing(false)
					.num_panels(num_columns as u64)
					.group_size(1)
					.build();
				let rows_to_skip = state.scroll / num_columns;
				let bytes_per_row = BYTES_PER_COLUMN * num_columns;
				let skip = rows_to_skip * bytes_per_row;
				let end = bytes
					.len()
					.min(skip + state.area.y.to_usize().unwrap() * BYTES_PER_COLUMN * num_columns);
				printer
					.display_offset(skip.try_into().unwrap())
					.print_all(&bytes[skip..end])
					.unwrap();
				let string = String::from_utf8(out).unwrap().into_text().unwrap();
				Paragraph::new(string)
					.wrap(Wrap { trim: false })
					.render(area, buf);
			},
		}
	}

	pub fn resize(&self, area: Rect) {
		let mut state = self.state.write().unwrap();
		let width = area.x.to_usize().unwrap();
		state.num_columns = Self::calc_num_columns(width);
		state.area = area;
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		let state = self.state.read().unwrap();
		state.area.contains(Position { x, y })
	}

	pub fn bottom(&self) {
		let mut state = self.state.write().unwrap();
		let sub = if let Either::Right(_) = state.content {
			state
				.area
				.height
				.to_usize()
				.unwrap()
				.saturating_sub(BYTES_PER_COLUMN)
				* state.num_columns.max(1)
		} else {
			state.area.height.to_usize().unwrap().saturating_sub(2)
		};
		state.scroll = state.line_count.saturating_sub(sub);
	}

	pub fn top(&self) {
		let mut state = self.state.write().unwrap();
		state.scroll = 0;
	}

	pub fn down(&self) {
		let mut state = self.state.write().unwrap();
		let height = state.area.height.to_usize().unwrap();
		let (scroll, max_scroll) = if let Either::Right(_) = state.content {
			(state.num_columns.max(1), state.line_count)
		} else {
			(1, state.line_count.saturating_sub(height))
		};
		state.scroll = (state.scroll + scroll).min(max_scroll);
	}

	pub fn up(&self) {
		let mut state = self.state.write().unwrap();
		let scroll = if let Either::Right(_) = state.content {
			state.num_columns.max(1)
		} else {
			1
		};
		state.scroll = state.scroll.saturating_sub(scroll);
	}

	async fn get_content(&self) -> tg::Result<Either<String, Bytes>> {
		// Get the data.
		let bytes = match &self.value {
			tg::Value::Object(tg::Object::Leaf(leaf)) => leaf.bytes(&self.handle).await?,
			tg::Value::Object(tg::Object::File(object)) => object.bytes(&self.handle).await?,
			_ => unreachable!(),
		};

		// Check to see if the data is valid UTF-8.
		if let Ok(str) = str::from_utf8(&bytes) {
			// Check to see if the UTF-8 string contains escape codes.
			let ansi_pattern = Regex::new(ANSI_REGEX).unwrap();
			if !ansi_pattern.is_match(str) {
				return Ok(Either::Left(str.to_owned()));
			}
		}
		Ok(Either::Right(bytes))
	}
}
