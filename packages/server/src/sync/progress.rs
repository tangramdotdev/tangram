use {
	crate::Server,
	futures::future,
	std::{pin::pin, sync::atomic::AtomicU64, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
};

#[derive(Debug)]
pub struct Progress {
	pub processes: AtomicU64,
	pub objects: AtomicU64,
	pub bytes: AtomicU64,
}

impl Server {
	pub(super) async fn sync_progress_task(
		&self,
		progress: &Progress,
		stop: Stop,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) {
		loop {
			let stop = stop.wait();
			let stop = pin!(stop);
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let sleep = pin!(sleep);
			let result = future::select(sleep, stop).await;
			let message = progress.get();
			let message = tg::sync::Message::Progress(message);
			sender.send(Ok(message)).await.ok();
			if matches!(result, future::Either::Right(_)) {
				break;
			}
		}
	}
}

impl Progress {
	pub fn new() -> Self {
		Self {
			processes: AtomicU64::new(0),
			objects: AtomicU64::new(0),
			bytes: AtomicU64::new(0),
		}
	}

	pub fn increment(&self, processes: u64, objects: u64, bytes: u64) {
		if processes > 0 {
			self.processes
				.fetch_update(
					std::sync::atomic::Ordering::SeqCst,
					std::sync::atomic::Ordering::SeqCst,
					|current| Some(current + processes),
				)
				.unwrap();
		}
		if objects > 0 {
			self.objects
				.fetch_update(
					std::sync::atomic::Ordering::SeqCst,
					std::sync::atomic::Ordering::SeqCst,
					|current| Some(current + objects),
				)
				.unwrap();
		}
		if bytes > 0 {
			self.bytes
				.fetch_update(
					std::sync::atomic::Ordering::SeqCst,
					std::sync::atomic::Ordering::SeqCst,
					|current| Some(current + bytes),
				)
				.unwrap();
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
