use {
	futures::{Stream, StreamExt as _},
	std::{
		sync::{Arc, atomic::AtomicU64},
		time::Duration,
	},
	tangram_client as tg,
	tokio_stream::wrappers::IntervalStream,
};

#[derive(Debug)]
pub struct Progress {
	pub processes: Option<AtomicU64>,
	pub objects: AtomicU64,
	pub bytes: AtomicU64,
}

impl Progress {
	pub fn new(processes: bool) -> Self {
		Self {
			processes: processes.then(|| AtomicU64::new(0)),
			objects: AtomicU64::new(0),
			bytes: AtomicU64::new(0),
		}
	}

	pub fn increment_processes(&self) {
		self.processes
			.as_ref()
			.unwrap()
			.fetch_update(
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
				|current| Some(current + 1),
			)
			.unwrap();
	}

	pub fn increment_objects(&self, objects: u64) {
		self.objects
			.fetch_update(
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
				|current| Some(current + objects),
			)
			.unwrap();
	}

	pub fn increment_bytes(&self, bytes: u64) {
		self.bytes
			.fetch_update(
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
				|current| Some(current + bytes),
			)
			.unwrap();
	}

	pub fn stream(self: Arc<Self>) -> impl Stream<Item = tg::Result<tg::import::Progress>> {
		let progress = self.clone();
		let interval = Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		IntervalStream::new(interval).skip(1).map(move |_| {
			let processes = progress
				.processes
				.as_ref()
				.map(|processes| processes.swap(0, std::sync::atomic::Ordering::SeqCst));
			let objects = progress
				.objects
				.swap(0, std::sync::atomic::Ordering::SeqCst);
			let bytes = progress.bytes.swap(0, std::sync::atomic::Ordering::SeqCst);
			Ok(tg::import::Progress {
				processes,
				objects,
				bytes,
			})
		})
	}
}
