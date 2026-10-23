use {
	crate::Component,
	futures::StreamExt as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_messenger::payload::Json,
	tangram_server_core::{Server, authentication::cache::Invalidation, messenger::Messenger},
};

impl Component {
	pub async fn invalidate_user_authentication(&self, server: &Server, users: &[tg::user::Id]) {
		for user in users {
			self.invalidate_authentication(
				server,
				Invalidation::Principal(tg::Principal::User(user.clone())),
			)
			.await;
		}
	}

	pub async fn invalidate_authentication(&self, server: &Server, invalidation: Invalidation) {
		if let Err(error) = self.cache.invalidate(invalidation.clone()).await {
			tracing::error!(error = %error.trace(), "failed to invalidate the local authentication cache");
			self.cache.set_ready(false);
		}
		Self::spawn_publish_authentication_invalidation_task(server, invalidation);
	}

	pub(crate) fn spawn_publish_authentication_invalidation_task(
		server: &Server,
		invalidation: Invalidation,
	) {
		let messenger = server.messenger.clone();
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
		server: &Server,
		mut events: tokio::sync::broadcast::Receiver<()>,
		started: tokio::sync::oneshot::Sender<()>,
	) {
		let mut started = Some(started);
		loop {
			self.cache.set_ready(false);
			if !server.messenger.connected() {
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
				server.messenger.subscribe_to_region::<Json<Invalidation>>(
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
					server.clock.sleep(Duration::from_secs(1)).await;
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
			if !server.messenger.connected()
				|| !matches!(
					events.try_recv(),
					Err(tokio::sync::broadcast::error::TryRecvError::Empty)
				) {
				continue;
			}
			self.cache.set_ready(true);
			if let Some(started) = started.take() {
				let _ = started.send(());
			}
			tangram_server_core::checkpoint!(server, "authentication.cache.ready").await;
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
						let (kind, id) = match &invalidation {
							Invalidation::Principal(principal) => ("principal", principal.to_string()),
							Invalidation::Token(hash) => ("token", blake3::Hash::from_bytes(*hash).to_hex().to_string()),
						};
						if let Err(error) = self.cache.invalidate(invalidation).await {
							tracing::error!(error = %error.trace(), "failed to invalidate an authentication entry");
							break;
						}
						tangram_server_core::checkpoint!(server, "authentication.cache.invalidated", id = %id, kind = %kind).await;
					},
				}
			}
			self.cache.set_ready(false);
			server.clock.sleep(Duration::from_secs(1)).await;
		}
	}
}
