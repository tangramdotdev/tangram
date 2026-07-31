use {
	num::ToPrimitive as _,
	ratatui::{self as tui, prelude::*},
	unicode_segmentation::UnicodeSegmentation as _,
	unicode_width::UnicodeWidthStr as _,
};

pub struct Data {
	contents: String,
	num_columns: usize,
	num_lines: usize,
	rect: Option<Rect>,
	scroll: (usize, usize),
	update_receiver: UpdateReceiver,
	update_sender: UpdateSender,
}

pub type UpdateSender = std::sync::mpsc::Sender<Box<dyn Send + FnOnce(&mut Data)>>;

pub type UpdateReceiver = std::sync::mpsc::Receiver<Box<dyn Send + FnOnce(&mut Data)>>;

impl Data {
	fn calculate_size(contents: &str) -> (usize, usize) {
		let mut num_lines = 0;
		let mut num_columns = 0;
		for line in contents.split('\n') {
			num_lines += 1;
			let line = line.strip_suffix('\r').unwrap_or(line);
			let mut c = 0;
			for grapheme in line.graphemes(false) {
				c += grapheme.width();
			}
			num_columns = num_columns.max(c);
		}
		(num_lines, num_columns)
	}

	pub fn down(&mut self) {
		let max = self
			.num_lines
			.saturating_sub(self.rect.map_or(0, |r| r.height.to_usize().unwrap()));
		self.scroll.0 = (self.scroll.0 + 1).min(max);
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		self.rect
			.is_some_and(|rect| rect.contains(Position { x, y }))
	}

	pub fn left(&mut self) {
		self.scroll.1 = self.scroll.1.saturating_sub(1);
	}

	pub fn new() -> Self {
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		Self {
			contents: String::new(),
			num_columns: 0,
			num_lines: 1,
			rect: None,
			scroll: (0, 0),
			update_receiver,
			update_sender,
		}
	}

	pub fn render(&mut self, rect: Rect, buffer: &mut Buffer) {
		self.rect.replace(rect);
		let row = self.scroll.0.to_u16().unwrap_or(u16::MAX);
		let column = self.scroll.1.to_u16().unwrap_or(u16::MAX);
		tui::widgets::Paragraph::new(self.contents.as_str())
			.scroll((row, column))
			.render(rect, buffer);
	}

	pub fn right(&mut self) {
		let max = self
			.num_columns
			.saturating_sub(self.rect.map_or(0, |r| r.width.to_usize().unwrap()));
		self.scroll.1 = (self.scroll.1 + 1).min(max);
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn set_contents(&mut self, contents: String) {
		self.contents = contents.replace('\t', "    ");
		let (num_lines, num_columns) = Self::calculate_size(&self.contents);
		self.num_lines = num_lines;
		self.num_columns = num_columns;
		self.scroll = (0, 0);
	}

	pub fn up(&mut self) {
		self.scroll.0 = self.scroll.0.saturating_sub(1);
	}

	pub fn update(&mut self) -> bool {
		let mut changed = false;
		while let Ok(update) = self.update_receiver.try_recv() {
			update(self);
			changed = true;
		}

		changed
	}

	pub fn update_sender(&self) -> UpdateSender {
		self.update_sender.clone()
	}
}
