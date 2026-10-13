use {
	crate::{Server, authentication::Entry, config, messenger::Messenger},
	futures::StreamExt as _,
	std::{
		sync::atomic::{AtomicBool, Ordering},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_messenger::payload::Json,
};

#[cfg(test)]
mod tests;

pub(crate) type Hash = [u8; 32];

pub(crate) struct Cache {
	entries: Option<moka::future::Cache<Hash, Entry>>,
	ready: AtomicBool,
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) enum Invalidation {
	Principal(tg::Principal),
	Token(Hash),
}

#[must_use]
pub(crate) fn deleted_users(batch: &tangram_index::batch::Arg) -> Vec<tg::user::Id> {
	batch
		.items
		.iter()
		.filter_map(|item| match item {
			tangram_index::batch::Item::DeleteUser(user) => Some(user.clone()),
			_ => None,
		})
		.collect()
}

impl Cache {
	#[must_use]
	pub(crate) fn new(config: &config::AuthenticationCache) -> Self {
		let entries = (config.capacity != 0).then(|| {
			moka::future::Cache::builder()
				.max_capacity(config.capacity)
				.time_to_live(config.ttl)
				.support_invalidation_closures()
				.build()
		});
		Self {
			entries,
			ready: AtomicBool::new(false),
		}
	}

	pub(crate) async fn get(&self, hash: &Hash) -> Option<Entry> {
		if !self.ready.load(Ordering::Acquire) {
			return None;
		}
		self.entries.as_ref()?.get(hash).await
	}

	pub(crate) async fn insert(&self, hash: Hash, entry: Entry) {
		if !self.ready.load(Ordering::Acquire)
			|| matches!(entry.principal, tg::Principal::User(_))
				&& entry.billing_ready != Some(true)
		{
			return;
		}
		if let Some(entries) = &self.entries {
			entries.insert(hash, entry).await;
		}
	}

	pub(crate) async fn invalidate(&self, invalidation: Invalidation) -> tg::Result<()> {
		let Some(entries) = &self.entries else {
			return Ok(());
		};
		match invalidation {
			Invalidation::Principal(principal) => {
				entries
					.invalidate_entries_if(move |_, entry| entry.principal == principal)
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to invalidate the principal's authentication entries"
						)
					})?;
			},
			Invalidation::Token(hash) => entries.invalidate(&hash).await,
		}
		Ok(())
	}

	pub(crate) fn set_ready(&self, ready: bool) {
		self.ready.store(false, Ordering::Release);
		if let Some(entries) = &self.entries {
			entries.invalidate_all();
		}
		self.ready.store(ready, Ordering::Release);
	}
}

impl Server {
	pub(crate) async fn invalidate_user_authentication(&self, users: &[tg::user::Id]) {
		for user in users {
			self.invalidate_authentication(Invalidation::Principal(tg::Principal::User(
				user.clone(),
			)))
			.await;
		}
	}

	pub(crate) async fn invalidate_authentication(&self, invalidation: Invalidation) {
		if let Err(error) = self
			.authentication_cache
			.invalidate(invalidation.clone())
			.await
		{
			tracing::error!(error = %error.trace(), "failed to invalidate the local authentication cache");
			self.authentication_cache.set_ready(false);
		}
		self.spawn_publish_authentication_invalidation_task(invalidation);
	}

	pub(crate) fn spawn_publish_authentication_invalidation_task(
		&self,
		invalidation: Invalidation,
	) {
		let messenger = self.messenger.clone();
		tokio::spawn(Self::publish_authentication_invalidation_task(
			messenger,
			invalidation,
		));
	}

	async fn publish_authentication_invalidation_task(
		messenger: Messenger,
		invalidation: Invalidation,
	) {
		for attempt in 0..=3 {
			if attempt != 0 {
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
			let result =
				Self::publish_authentication_invalidation_with_messenger(&messenger, &invalidation)
					.await;
			match result {
				Ok(()) => return,
				Err(error) if attempt == 0 => {
					tracing::warn!(error = %error.trace(), "failed to publish an authentication invalidation; retrying");
				},
				Err(error) if attempt == 3 => {
					tracing::error!(error = %error.trace(), "failed to publish an authentication invalidation after 3 retries");
				},
				Err(_) => {},
			}
		}
	}

	async fn publish_authentication_invalidation_with_messenger(
		messenger: &Messenger,
		invalidation: &Invalidation,
	) -> tg::Result<()> {
		let result = tokio::time::timeout(
			Duration::from_secs(1),
			messenger.publish_to_region(
				None,
				"authentication.invalidate".into(),
				Json(invalidation.clone()),
			),
		)
		.await
		.map_err(|error| {
			tg::error!(
				!error,
				"timed out publishing the authentication invalidation"
			)
		})?;
		result.map_err(|error| {
			tg::error!(!error, "failed to publish the authentication invalidation")
		})?;
		Ok(())
	}

	pub(crate) async fn authentication_cache_task(
		&self,
		mut events: tokio::sync::broadcast::Receiver<()>,
		started: tokio::sync::oneshot::Sender<()>,
	) {
		let mut started = Some(started);
		loop {
			self.authentication_cache.set_ready(false);
			if !self.messenger.connected() {
				if let Some(started) = started.take() {
					let _ = started.send(());
				}
				if events.recv().await.is_err_and(|error| {
					matches!(error, tokio::sync::broadcast::error::RecvError::Closed)
				}) {
					return;
				}
				continue;
			}
			while events.try_recv().is_ok() {}
			let result = tokio::time::timeout(
				Duration::from_secs(1),
				self.messenger.subscribe_to_region::<Json<Invalidation>>(
					None,
					"authentication.invalidate".into(),
				),
			)
			.await;
			let messages = match result {
				Ok(Ok(messages)) => messages,
				Ok(Err(error)) => {
					let error = tg::error!(
						!error,
						"failed to subscribe to authentication invalidations"
					);
					tracing::error!(error = %error.trace(), "failed to subscribe to authentication invalidations");
					if let Some(started) = started.take() {
						let _ = started.send(());
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
				Err(_) => {
					if let Some(started) = started.take() {
						let _ = started.send(());
					}
					continue;
				},
			};
			let mut messages = std::pin::pin!(messages);
			// Reestablish the subscription if the connection changed while subscribing.
			if !self.messenger.connected()
				|| !matches!(
					events.try_recv(),
					Err(tokio::sync::broadcast::error::TryRecvError::Empty)
				) {
				continue;
			}
			self.authentication_cache.set_ready(true);
			if let Some(started) = started.take() {
				let _ = started.send(());
			}
			loop {
				tokio::select! {
					_ = events.recv() => break,
					message = messages.next() => {
						let invalidation = match message {
							Some(Ok(message)) => message.payload.0,
							Some(Err(error)) => {
								tracing::error!(%error, "failed to receive an authentication invalidation");
								break;
							},
							None => break,
						};
						if let Err(error) = self.authentication_cache.invalidate(invalidation).await {
							tracing::error!(error = %error.trace(), "failed to invalidate an authentication entry");
							break;
						}
					},
				}
			}
			self.authentication_cache.set_ready(false);
			tokio::time::sleep(Duration::from_secs(1)).await;
		}
	}
}
