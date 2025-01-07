use num::ToPrimitive;
use ratatui::{self as tui, prelude::*};

pub struct Data {
	pub contents: String,
	update_sender: UpdateSender,
	update_receiver: UpdateReceiver,
	scroll: usize,
	rect: Option<Rect>,
}

pub type UpdateSender = std::sync::mpsc::Sender<Box<dyn FnOnce(&mut Data)>>;
pub type UpdateReceiver = std::sync::mpsc::Receiver<Box<dyn FnOnce(&mut Data)>>;

impl Data {
	pub fn new() -> Self {
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		Self {
			contents: String::new(),
			update_sender,
			update_receiver,
			scroll: 0,
			rect: None,
		}
	}

	pub fn update(&mut self) {
		while let Ok(update) = self.update_receiver.try_recv() {
			update(self);
		}
	}

	pub fn render(&mut self, rect: Rect, buffer: &mut Buffer) {
		let lines = self
			.contents
			.lines()
			.skip(self.scroll)
			.map(Line::from)
			.collect::<Vec<_>>();
		let paragraph = tui::widgets::Paragraph::new(lines);
		paragraph.render(rect, buffer);
		self.rect.replace(rect);
	}

	pub fn update_sender(&self) -> UpdateSender {
		self.update_sender.clone()
	}

	pub fn up(&mut self) {
		self.scroll = self.scroll.saturating_sub(1);
	}

	pub fn down(&mut self) {
		let max = self.rect.map_or(0, |rect| {
			self.contents
				.lines()
				.count()
				.saturating_sub(rect.height.to_usize().unwrap())
		});
		self.scroll = (self.scroll + 1).min(max);
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		self.rect
			.map_or(false, |rect| rect.contains(Position { x, y }))
	}
}
