use {
	crate::Session,
	futures::future,
	std::{pin::pin, sync::atomic::AtomicU64, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
};

#[derive(Debug)]
pub struct Progress {
	skipped: Amounts,
	transferred: Amounts,
}

#[derive(Debug)]
struct Amounts {
	bytes: AtomicU64,
	groups: AtomicU64,
	objects: AtomicU64,
	organizations: AtomicU64,
	processes: AtomicU64,
	sandboxes: AtomicU64,
	tags: AtomicU64,
	users: AtomicU64,
}

impl Session {
	pub(super) async fn sync_get_progress_task(
		&self,
		progress: &Progress,
		stopper: Stopper,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
	) {
		loop {
			let stopper = stopper.wait();
			let stopper = pin!(stopper);
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let sleep = pin!(sleep);
			let result = future::select(sleep, stopper).await;
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
		stopper: Stopper,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::sync::PutMessage>>,
	) {
		loop {
			let stopper = stopper.wait();
			let stopper = pin!(stopper);
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let sleep = pin!(sleep);
			let result = future::select(sleep, stopper).await;
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
			skipped: Amounts {
				bytes: AtomicU64::new(0),
				groups: AtomicU64::new(0),
				objects: AtomicU64::new(0),
				organizations: AtomicU64::new(0),
				processes: AtomicU64::new(0),
				sandboxes: AtomicU64::new(0),
				tags: AtomicU64::new(0),
				users: AtomicU64::new(0),
			},
			transferred: Amounts {
				bytes: AtomicU64::new(0),
				groups: AtomicU64::new(0),
				objects: AtomicU64::new(0),
				organizations: AtomicU64::new(0),
				processes: AtomicU64::new(0),
				sandboxes: AtomicU64::new(0),
				tags: AtomicU64::new(0),
				users: AtomicU64::new(0),
			},
		}
	}

	pub fn increment_skipped(&self, processes: u64, objects: u64, bytes: u64) {
		if processes > 0 {
			self.skipped
				.processes
				.fetch_add(processes, std::sync::atomic::Ordering::SeqCst);
		}
		if objects > 0 {
			self.skipped
				.objects
				.fetch_add(objects, std::sync::atomic::Ordering::SeqCst);
		}
		if bytes > 0 {
			self.skipped
				.bytes
				.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
		}
	}

	pub fn increment_transferred(&self, processes: u64, objects: u64, bytes: u64) {
		if processes > 0 {
			self.transferred
				.processes
				.fetch_add(processes, std::sync::atomic::Ordering::SeqCst);
		}
		if objects > 0 {
			self.transferred
				.objects
				.fetch_add(objects, std::sync::atomic::Ordering::SeqCst);
		}
		if bytes > 0 {
			self.transferred
				.bytes
				.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
		}
	}

	pub fn increment_transferred_node(&self, id: &tg::Id) {
		let amount = match id.kind() {
			tg::id::Kind::Group => &self.transferred.groups,
			tg::id::Kind::Organization => &self.transferred.organizations,
			tg::id::Kind::Process => &self.transferred.processes,
			tg::id::Kind::Sandbox => &self.transferred.sandboxes,
			tg::id::Kind::Tag => &self.transferred.tags,
			tg::id::Kind::User => &self.transferred.users,
			kind if kind.is_object() => &self.transferred.objects,
			_ => return,
		};
		amount.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
	}

	pub fn reset(&self) -> tg::sync::ProgressMessage {
		let skipped = tg::sync::ProgressMessageAmounts {
			bytes: self
				.skipped
				.bytes
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			groups: self
				.skipped
				.groups
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			objects: self
				.skipped
				.objects
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			organizations: self
				.skipped
				.organizations
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			processes: self
				.skipped
				.processes
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			sandboxes: self
				.skipped
				.sandboxes
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			tags: self
				.skipped
				.tags
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			users: self
				.skipped
				.users
				.swap(0, std::sync::atomic::Ordering::SeqCst),
		};
		let transferred = tg::sync::ProgressMessageAmounts {
			bytes: self
				.transferred
				.bytes
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			groups: self
				.transferred
				.groups
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			objects: self
				.transferred
				.objects
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			organizations: self
				.transferred
				.organizations
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			processes: self
				.transferred
				.processes
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			sandboxes: self
				.transferred
				.sandboxes
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			tags: self
				.transferred
				.tags
				.swap(0, std::sync::atomic::Ordering::SeqCst),
			users: self
				.transferred
				.users
				.swap(0, std::sync::atomic::Ordering::SeqCst),
		};
		tg::sync::ProgressMessage {
			skipped,
			transferred,
		}
	}
}
