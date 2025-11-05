use {std::sync::atomic::AtomicU64, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Progress {
	pub processes: AtomicU64,
	pub objects: AtomicU64,
	pub bytes: AtomicU64,
}

impl Progress {
	pub fn new() -> Self {
		Self {
			processes: AtomicU64::new(0),
			objects: AtomicU64::new(0),
			bytes: AtomicU64::new(0),
		}
	}

	pub fn increment_processes(&self) {
		self.processes
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

	pub fn get(&self) -> tg::sync::ProgressMessage {
		let processes = self.processes.swap(0, std::sync::atomic::Ordering::SeqCst);
		let objects = self.objects.swap(0, std::sync::atomic::Ordering::SeqCst);
		let bytes = self.bytes.swap(0, std::sync::atomic::Ordering::SeqCst);
		tg::sync::ProgressMessage {
			processes,
			objects,
			bytes,
		}
	}
}
