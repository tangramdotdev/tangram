#![allow(dead_code)]
use layout::Position;
use num::ToPrimitive as _;
use ratatui::{
	prelude::*,
	widgets::{Paragraph, Wrap},
};
use std::sync::{Arc, RwLock};
use tangram_client as tg;

pub struct Data<H> {
	handle: H,
	value: tg::Value,
	state: RwLock<State>,
}

struct State {
	area: Rect,
	text: String,
	scroll: usize,
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
			text: String::new(),
			scroll: 0,
		});
		let info = Arc::new(Self {
			handle,
			value,
			state,
		});
		tokio::task::spawn({
			let info = info.clone();
			async move {
				let text = info
					.get_text()
					.await
					.unwrap_or_else(|error| format!("error: {error}"));
				info.state.write().unwrap().text = text;
			}
		});
		info
	}

	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		let state = self.state.read().unwrap();
		let lines = state
			.text
			.lines()
			.skip(state.scroll)
			.map(Line::raw)
			.collect::<Vec<_>>();
		Paragraph::new(lines)
			.wrap(Wrap { trim: false })
			.render(area, buf);
	}

	pub fn resize(&self, area: Rect) {
		let mut state = self.state.write().unwrap();
		state.area = area;
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		let state = self.state.read().unwrap();
		state.area.contains(Position { x, y })
	}

	pub fn bottom(&self) {
		let mut state = self.state.write().unwrap();
		let num_lines = state.text.lines().count();
		state.scroll =
			num_lines.saturating_sub(state.area.height.to_usize().unwrap().saturating_sub(2));
	}

	pub fn top(&self) {
		let mut state = self.state.write().unwrap();
		state.scroll = 0;
	}

	pub fn down(&self) {
		let mut state = self.state.write().unwrap();
		let height = state.area.height.to_usize().unwrap();
		let num_lines = state.text.lines().count();
		let max_scroll = num_lines.saturating_sub(height);
		state.scroll = (state.scroll + 1).min(max_scroll);
	}

	pub fn up(&self) {
		let mut state = self.state.write().unwrap();
		state.scroll = state.scroll.saturating_sub(1);
	}

	async fn get_text(&self) -> tg::Result<String> {
		match &self.value {
			tg::Value::Object(tg::Object::Leaf(leaf)) => {
				let bytes = leaf.bytes(&self.handle).await?;
				Ok(String::from_utf8_lossy(&bytes).into())
			},
			tg::Value::Object(tg::Object::File(object)) => {
				let text = object.text(&self.handle).await?;
				Ok(text)
			},
			_ => unreachable!(),
		}
	}
}
