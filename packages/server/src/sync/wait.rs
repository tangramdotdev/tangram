use {
	crate::{
		Session,
		sync::get::notification::{self, Request, Response},
	},
	futures::TryStreamExt as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
};

impl Session {
	pub(crate) async fn try_get_with_sync_wait<T, F, Fut>(
		&self,
		tokens: &tg::Tokens,
		request: tg::sync::notification::Request,
		mut f: F,
	) -> tg::Result<Option<T>>
	where
		F: FnMut() -> Fut,
		Fut: Future<Output = tg::Result<Option<T>>>,
	{
		if let Some(value) = f().await? {
			return Ok(Some(value));
		}
		let Some(token) = tokens
			.local_sync()
			.filter(|token| self.verify_sync_token(token))
		else {
			return Ok(None);
		};
		let subject = notification::subject(token);
		let responses = self
			.server
			.messenger
			.subscribe::<Response>(format!("{subject}.client.{}", request.id()))
			.await
			.map_err(|error| tg::error!(!error, "failed to subscribe to the sync response"))?;
		let request = Request(request);
		let mut responses = pin!(responses);
		let timeout = self.server.config.sync.item_get_timeout;
		let deadline = tokio::time::Instant::now() + timeout;

		// Resend the request on an interval and retry the get after each answer or interval, since the sync may not be subscribed when the request is sent and the get may need the index the sync writes when it ends.
		loop {
			self.server
				.messenger
				.publish(format!("{subject}.server"), request.clone())
				.await
				.map_err(|error| tg::error!(!error, "failed to publish the sync request"))?;
			let interval = std::cmp::min(
				self.server.config.sync.item_get_interval,
				deadline.saturating_duration_since(tokio::time::Instant::now()),
			);
			let answered = match tokio::time::timeout(interval, responses.try_next()).await {
				Ok(Ok(Some(message))) => {
					if !message.payload.0.available() {
						return Ok(None);
					}
					true
				},
				Ok(Ok(None)) => return Ok(None),
				Ok(Err(error)) => {
					return Err(tg::error!(!error, "failed to receive the sync response"));
				},
				Err(_) => false,
			};
			if let Some(value) = f().await? {
				return Ok(Some(value));
			}
			if tokio::time::Instant::now() >= deadline {
				return Ok(None);
			}
			if answered {
				tokio::time::sleep(interval).await;
			}
		}
	}
}
