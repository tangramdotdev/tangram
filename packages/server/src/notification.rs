use {
	crate::{Config, Server, messenger::Messenger},
	dashmap::{DashMap, mapref::entry::Entry},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
};

#[derive(Clone)]
pub(crate) struct Notifications {
	messenger: Messenger,
	pending: Arc<DashMap<Key, bool>>,
	regions: Vec<String>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct Key {
	subject: String,
	target: Target,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Target {
	Current,
	Region(Option<String>),
}

impl Notifications {
	#[must_use]
	pub(crate) fn new(messenger: Messenger, config: &Config) -> Self {
		let pending = Arc::default();
		let regions = crate::database::index::queue::database_index_queue_regions(config)
			.into_iter()
			.collect();

		Self {
			messenger,
			pending,
			regions,
		}
	}

	pub(crate) fn notify_database_index_queue(&self) {
		for region in &self.regions {
			let region = (!region.is_empty()).then(|| region.clone());
			let subject = crate::indexer::database_index_queue_subject();
			self.notify_deduplicated(Target::Region(region), subject);
		}
	}

	pub(crate) fn notify_log_compaction(&self) {
		let subject = crate::indexer::log_compaction_subject();
		self.notify_deduplicated(Target::Current, subject);
	}

	pub(crate) fn notify_process_log(&self, id: &tg::process::Id) {
		let subject = format!("processes.{id}.log");
		self.notify_deduplicated(Target::Current, subject);
	}

	pub(crate) fn notify_process_status(&self, id: &tg::process::Id) {
		let subject = format!("processes.{id}.status");
		self.notify_deduplicated(Target::Current, subject);
	}

	pub(crate) fn notify_process_stdio_close(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		let subject = format!("processes.{id}.{stream}.close");
		self.spawn_publish(subject);
	}

	pub(crate) fn notify_sandbox_status(&self, id: &tg::sandbox::Id) {
		let subject = format!("sandboxes.{id}.status");
		self.notify_deduplicated(Target::Current, subject);
	}

	pub(crate) async fn publish_process_child_spawned(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.children");
		self.messenger.publish(subject, ()).await.map_err(|error| {
			tg::error!(!error, "failed to publish the child spawned notification")
		})?;

		Ok(())
	}

	pub(crate) async fn publish_sandbox_control_connected(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let subject = crate::sandbox::control::connected_subject(id);
		self.messenger.publish(subject, ()).await.map_err(|error| {
			tg::error!(!error, "failed to publish the sandbox control connection")
		})?;

		Ok(())
	}

	pub(crate) async fn publish_sandbox_process_spawned(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let subject = format!("sandboxes.{id}.processes");
		self.messenger.publish(subject, ()).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to publish the sandbox process spawned notification"
			)
		})?;

		Ok(())
	}

	pub(crate) async fn publish_scheduler_heartbeat(
		&self,
		id: &tg::scheduler::Id,
	) -> tg::Result<()> {
		let subject = crate::scheduler::scheduler_heartbeat_subject(id);
		self.messenger
			.publish(subject, ())
			.await
			.map_err(|error| tg::error!(!error, "failed to publish the scheduler heartbeat"))?;

		Ok(())
	}

	fn notify_deduplicated(&self, target: Target, subject: String) {
		let key = Key { subject, target };
		match self.pending.entry(key.clone()) {
			Entry::Occupied(mut entry) => {
				*entry.get_mut() = true;
			},
			Entry::Vacant(entry) => {
				entry.insert(false);
				let messenger = self.messenger.clone();
				let pending = self.pending.clone();
				tokio::spawn(Self::publish_deduplicated(messenger, pending, key));
			},
		}
	}

	async fn publish_deduplicated(
		messenger: Messenger,
		pending: Arc<DashMap<Key, bool>>,
		key: Key,
	) {
		loop {
			let result = match &key.target {
				Target::Current => messenger.publish(key.subject.clone(), ()).await,
				Target::Region(region) => {
					messenger
						.publish_to_region(region.as_deref(), key.subject.clone(), ())
						.await
				},
			};
			if let Err(error) = result {
				tracing::error!(%error, subject = %key.subject, target = ?key.target, "failed to publish a notification");
			}
			// Keep one publisher active while coalescing notifications received during publication.
			let repeat = match pending.entry(key.clone()) {
				Entry::Occupied(mut entry) if *entry.get() => {
					*entry.get_mut() = false;
					true
				},
				Entry::Occupied(entry) => {
					entry.remove();
					false
				},
				Entry::Vacant(_) => unreachable!(),
			};
			if !repeat {
				break;
			}
		}
	}

	fn spawn_publish(&self, subject: String) {
		let messenger = self.messenger.clone();
		tokio::spawn(async move {
			if let Err(error) = messenger.publish(subject.clone(), ()).await {
				tracing::error!(%error, %subject, "failed to publish a notification");
			}
		});
	}
}

impl Server {
	pub(crate) fn spawn_publish_database_index_queue_notification_task(&self) {
		self.notifications.notify_database_index_queue();
	}

	pub(crate) fn spawn_publish_log_compaction_notification_task(&self) {
		self.notifications.notify_log_compaction();
	}

	pub(crate) fn spawn_publish_process_status_task(&self, id: &tg::process::Id) {
		self.notifications.notify_process_status(id);
	}

	pub(crate) fn spawn_publish_process_stdio_close_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		self.notifications.notify_process_stdio_close(id, stream);
	}

	pub(crate) fn spawn_publish_sandbox_status_task(&self, id: &tg::sandbox::Id) {
		self.notifications.notify_sandbox_status(id);
	}
}
