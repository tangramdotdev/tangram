use {
	std::sync::atomic::{AtomicUsize, Ordering},
	tangram_client::prelude::*,
	tangram_either::Either,
};

pub struct ProcessItem {
	pub eager: bool,
	pub id: tg::process::Id,
	pub parent: Option<tg::process::Id>,
}

pub struct ObjectItem {
	pub eager: bool,
	pub id: tg::object::Id,
	pub parent: Option<Either<tg::process::Id, tg::object::Id>>,
}

pub struct Queue {
	counter: AtomicUsize,
	object_sender: async_channel::Sender<ObjectItem>,
	process_sender: async_channel::Sender<ProcessItem>,
}

impl Queue {
	pub fn new(
		process_sender: async_channel::Sender<ProcessItem>,
		object_sender: async_channel::Sender<ObjectItem>,
	) -> Self {
		let counter = AtomicUsize::new(0);
		Self {
			counter,
			object_sender,
			process_sender,
		}
	}

	pub fn enqueue_process(&self, item: ProcessItem) {
		self.increment(1);
		self.process_sender.force_send(item).unwrap();
	}

	pub fn enqueue_object(&self, item: ObjectItem) {
		self.increment(1);
		self.object_sender.force_send(item).unwrap();
	}

	pub fn enqueue_processes(&self, items: impl IntoIterator<Item = ProcessItem>) {
		let items: Vec<_> = items.into_iter().collect();
		self.increment(items.len());
		for item in items {
			self.process_sender.force_send(item).unwrap();
		}
	}

	pub fn enqueue_objects(&self, items: impl IntoIterator<Item = ObjectItem>) {
		let items: Vec<_> = items.into_iter().collect();
		self.increment(items.len());
		for item in items {
			self.object_sender.force_send(item).unwrap();
		}
	}

	pub fn increment(&self, n: usize) {
		self.counter.fetch_add(n, Ordering::SeqCst);
	}

	pub fn decrement(&self, n: usize) {
		let previous = self.counter.fetch_sub(n, Ordering::SeqCst);
		if previous == n {
			tracing::trace!("closing the queue");
			self.process_sender.close();
			self.object_sender.close();
		}
	}

	pub fn close_if_empty(&self) {
		if self.counter.load(Ordering::SeqCst) == 0 {
			tracing::trace!("closing the queue");
			self.process_sender.close();
			self.object_sender.close();
		}
	}
}
