use {
	crate::messenger::Messenger, dashmap::DashSet, std::sync::Arc, tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
};

#[derive(Clone)]
pub(crate) struct Notifications {
	pending: Arc<DashSet<tg::process::Id, tg::id::BuildHasher>>,
	sender: tokio::sync::mpsc::UnboundedSender<tg::process::Id>,
}

impl Notifications {
	#[must_use]
	pub(crate) fn new(messenger: Messenger) -> Self {
		let pending = Arc::<DashSet<_, tg::id::BuildHasher>>::default();
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		tokio::spawn({
			let pending = pending.clone();
			async move {
				while let Some(id) = receiver.recv().await {
					pending.remove(&id);
					messenger
						.publish(format!("processes.{id}.log"), ())
						.await
						.inspect_err(|error| {
							tracing::error!(%error, %id, "failed to publish the process log notification");
						})
						.ok();
				}
			}
		});

		Self { pending, sender }
	}

	pub(crate) fn notify(&self, id: &tg::process::Id) {
		if !self.pending.insert(id.clone()) {
			return;
		}
		if self.sender.send(id.clone()).is_err() {
			self.pending.remove(id);
		}
	}
}
