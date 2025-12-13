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
	pub(super) async fn sync_get_progress_task(
		&self,
		progress: &Progress,
		stop: Stop,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
	) {
		loop {
			let stop = stop.wait();
			let stop = pin!(stop);
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let sleep = pin!(sleep);
			let result = future::select(sleep, stop).await;
			let message = progress.reset();
			if message != tg::sync::ProgressMessage::default() {
				let message = tg::sync::GetMessage::Progress(message);
				sender.send(Ok(message)).await.ok();
			}
			if matches!(result, future::Either::Right(_)) {
				break;
			}
		}
	}

	pub(super) async fn sync_put_progress_task(
		&self,
		progress: &Progress,
		stop: Stop,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::sync::PutMessage>>,
	) {
		loop {
			let stop = stop.wait();
			let stop = pin!(stop);
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let sleep = pin!(sleep);
			let result = future::select(sleep, stop).await;
			let message = progress.reset();
			if message != tg::sync::ProgressMessage::default() {
				let message = tg::sync::PutMessage::Progress(message);
				sender.send(Ok(message)).await.ok();
			}
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
				.fetch_add(processes, std::sync::atomic::Ordering::SeqCst);
		}
		if objects > 0 {
			self.objects
				.fetch_add(objects, std::sync::atomic::Ordering::SeqCst);
		}
		if bytes > 0 {
			self.bytes
				.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
		}
	}

	pub fn reset(&self) -> tg::sync::ProgressMessage {
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
