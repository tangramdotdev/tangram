use {tangram_client::prelude::*, tangram_either::Either};

pub struct Queue {
	object_sender: async_channel::Sender<ObjectItem>,
	process_sender: async_channel::Sender<ProcessItem>,
}

pub struct ObjectItem {
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<ObjectKind>,
	pub parent: Option<Either<tg::object::Id, tg::process::Id>>,
}

pub struct ProcessItem {
	pub eager: bool,
	pub id: tg::process::Id,
	pub parent: Option<tg::process::Id>,
}

#[derive(Clone, Copy, Debug)]
pub enum ObjectKind {
	Command,
	Error,
	Log,
	Output,
}

impl Queue {
	pub fn new(
		object_sender: async_channel::Sender<ObjectItem>,
		process_sender: async_channel::Sender<ProcessItem>,
	) -> Self {
		Self {
			object_sender,
			process_sender,
		}
	}

	pub fn enqueue_object(&self, item: ObjectItem) {
		self.object_sender.force_send(item).ok();
	}

	pub fn enqueue_process(&self, item: ProcessItem) {
		self.process_sender.force_send(item).ok();
	}

	pub fn enqueue_objects(&self, items: impl IntoIterator<Item = ObjectItem>) {
		let items: Vec<_> = items.into_iter().collect();
		for item in items {
			self.object_sender.force_send(item).ok();
		}
	}

	pub fn enqueue_processes(&self, items: impl IntoIterator<Item = ProcessItem>) {
		let items: Vec<_> = items.into_iter().collect();
		for item in items {
			self.process_sender.force_send(item).ok();
		}
	}

	pub fn close(&self) {
		self.object_sender.close();
		self.process_sender.close();
	}
}
