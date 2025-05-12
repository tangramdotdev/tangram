use num::ToPrimitive;
use ratatui::{self as tui, prelude::*};
use unicode_segmentation::UnicodeSegmentation;

pub struct Data {
	contents: String,
	num_lines: usize,
	scroll: (usize, usize),
	num_columns: usize,
	update_sender: UpdateSender,
	update_receiver: UpdateReceiver,
	rect: Option<Rect>,
}

pub type UpdateSender = std::sync::mpsc::Sender<Box<dyn Send + FnOnce(&mut Data)>>;

pub type UpdateReceiver = std::sync::mpsc::Receiver<Box<dyn Send + FnOnce(&mut Data)>>;

impl Data {
	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		self.rect
			.is_some_and(|rect| rect.contains(Position { x, y }))
	}

	pub fn new() -> Self {
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		Self {
			contents: String::new(),
			update_sender,
			update_receiver,
			scroll: (0, 0),
			num_columns: 0,
			num_lines: 0,
			rect: None,
		}
	}

	#[allow(clippy::needless_pass_by_value)]
	pub fn set_contents(&mut self, contents: String) {
		self.contents = contents.replace('\t', "    ");
		let width = self.rect.map(|rect| rect.width.to_usize().unwrap());
		let (num_columns, num_lines) = Self::calculate_size(width, &self.contents);
		self.num_lines = num_lines;
		self.num_columns = num_columns;
		self.scroll = (0, 0);
	}

	fn calculate_size(width: Option<usize>, contents: &str) -> (usize, usize) {
		let mut num_lines = 0;
		let mut num_columns = 0;
		for line in contents.lines() {
			num_lines += 1;
			let columns = line.graphemes(false).count();
			if width.is_some_and(|width| width > columns) {
				num_lines += 1;
			} else {
				num_columns = num_columns.max(columns);
			}
		}
		(num_lines, num_columns)
	}

	pub fn render(&mut self, rect: Rect, buffer: &mut Buffer) {
		self.rect.replace(rect);
		tui::widgets::Paragraph::new(self.contents.clone())
			.scroll((
				self.scroll.0.to_u16().unwrap(),
				self.scroll.1.to_u16().unwrap(),
			))
			.render(rect, buffer);
	}

	pub fn up(&mut self) {
		self.scroll.0 = self.scroll.0.saturating_sub(1);
	}

	pub fn down(&mut self) {
		self.scroll.0 = (self.scroll.0 + 1).min(self.num_lines);
	}

	pub fn left(&mut self) {
		self.scroll.1 = self.scroll.1.saturating_sub(1);
	}

	pub fn right(&mut self) {
		self.scroll.1 = (self.scroll.1 + 1).min(self.num_columns);
	}

	pub fn update(&mut self) {
		while let Ok(update) = self.update_receiver.try_recv() {
			update(self);
		}
	}

	pub fn update_sender(&self) -> UpdateSender {
		self.update_sender.clone()
	}
}
