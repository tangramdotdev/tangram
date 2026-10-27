use {
	crate::messenger::Messenger,
	dashmap::{DashMap, mapref::entry::Entry},
	std::sync::Arc,
	tangram_messenger::Messenger as _,
};

#[derive(Clone)]
pub struct Notifications {
	messenger: Messenger,
	pending: Arc<DashMap<Key, bool>>,
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
	pub(crate) fn new(messenger: Messenger) -> Self {
		let pending = Arc::default();
		Self { messenger, pending }
	}

	/// Publish a notification on the current server, coalescing it with a pending notification for the same subject.
	pub fn notify(&self, subject: String) {
		self.notify_deduplicated(Target::Current, subject);
	}

	/// Publish a notification to a region, coalescing it with a pending notification for the same subject.
	pub fn notify_region(&self, region: Option<String>, subject: String) {
		self.notify_deduplicated(Target::Region(region), subject);
	}

	/// Publish a notification without coalescing.
	pub fn spawn_publish(&self, subject: String) {
		let messenger = self.messenger.clone();
		tokio::spawn(async move {
			if let Err(error) = messenger.publish(subject.clone(), ()).await {
				tracing::error!(%error, %subject, "failed to publish a notification");
			}
		});
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
}
