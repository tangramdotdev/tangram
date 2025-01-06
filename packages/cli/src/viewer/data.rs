use ratatui::{self as tui, prelude::*};

pub struct Data {
	pub contents: String,
	update_sender: UpdateSender,
	update_receiver: UpdateReceiver,
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
		}
	}

	pub fn update(&mut self) {
		while let Ok(update) = self.update_receiver.try_recv() {
			update(self);
		}
	}

	pub fn render(&self, rect: Rect, buffer: &mut Buffer) {
		let paragraph = tui::widgets::Paragraph::new(self.contents.as_str());
		paragraph.render(rect, buffer);
	}

	pub fn update_sender(&self) -> UpdateSender {
		self.update_sender.clone()
	}
}
