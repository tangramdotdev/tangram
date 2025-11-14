use {
	std::sync::atomic::{AtomicBool, AtomicUsize, Ordering},
	tangram_client::prelude::*,
	tangram_either::Either,
};

pub struct ProcessItem {
	pub parent: Option<tg::process::Id>,
	pub process: tg::process::Id,
	pub eager: bool,
}

pub struct ObjectItem {
	pub parent: Option<Either<tg::process::Id, tg::object::Id>>,
	pub object: tg::object::Id,
	pub eager: bool,
}

pub struct Queue {
	process_sender: async_channel::Sender<ProcessItem>,
	object_sender: async_channel::Sender<ObjectItem>,
	pending: AtomicUsize,
	end: AtomicBool,
}

impl Queue {
	pub fn new(
		process_sender: async_channel::Sender<ProcessItem>,
		object_sender: async_channel::Sender<ObjectItem>,
	) -> Self {
		Self {
			process_sender,
			object_sender,
			pending: AtomicUsize::new(0),
			end: AtomicBool::new(false),
		}
	}

	pub fn enqueue_process(&self, item: ProcessItem) {
		self.pending.fetch_add(1, Ordering::SeqCst);
		self.process_sender.force_send(item).unwrap();
	}

	pub fn enqueue_object(&self, item: ObjectItem) {
		self.pending.fetch_add(1, Ordering::SeqCst);
		self.object_sender.force_send(item).unwrap();
	}

	pub fn enqueue_processes(&self, items: impl IntoIterator<Item = ProcessItem>) {
		let items: Vec<_> = items.into_iter().collect();
		self.pending.fetch_add(items.len(), Ordering::SeqCst);
		for item in items {
			self.process_sender.force_send(item).unwrap();
		}
	}

	pub fn enqueue_objects(&self, items: impl IntoIterator<Item = ObjectItem>) {
		let items: Vec<_> = items.into_iter().collect();
		self.pending.fetch_add(items.len(), Ordering::SeqCst);
		for item in items {
			self.object_sender.force_send(item).unwrap();
		}
	}

	pub fn ack(&self, count: usize) {
		let previous = self.pending.fetch_sub(count, Ordering::SeqCst);
		if previous == count && self.end.load(Ordering::SeqCst) {
			self.process_sender.close();
			self.object_sender.close();
		}
	}

	pub fn end(&self) {
		self.end.store(true, Ordering::SeqCst);
		if self.pending.load(Ordering::SeqCst) == 0 {
			self.process_sender.close();
			self.object_sender.close();
		}
	}
}
