use {
	crate::{
		Session,
		sync::control::{Client, Output},
	},
	futures::FutureExt as _,
	std::sync::Arc,
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) async fn try_get_with_sync_wait<T, F, Fut>(
		&self,
		tokens: &tg::Tokens,
		arg: tg::sync::control::ClientRequestArg,
		mut f: F,
	) -> tg::Result<Option<T>>
	where
		F: FnMut(Option<tg::sync::control::GetServerResponseOutput>) -> Fut,
		Fut: Future<Output = tg::Result<Option<T>>>,
	{
		if let Some(value) = f(None).await? {
			return Ok(Some(value));
		}
		let Some(token) = tokens
			.local_sync()
			.filter(|token| self.verify_sync_token(token))
		else {
			return Ok(None);
		};
		if arg.node().is_none() {
			return Err(tg::error!("expected a sync node request"));
		}
		let client = self
			.sync_control
			.clone()
			.unwrap_or_else(|| Arc::new(Client::default()));
		let mut request = client.request(self, token, arg);
		let config = &self.server.config.sync.control;
		let mut retry = tokio::time::interval(config.retry_interval);
		retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

		// Check locally until the request is retained, including when the incoming sync has already ended.
		let response = loop {
			let acknowledged = match request.output() {
				Output::Pending { acknowledged } => acknowledged,
				Output::Ready(result) => break result,
			};
			let changed = request.changed().boxed();
			tokio::select! {
				result = changed => {
					if let Err(error) = result {
						break Err(error);
					}
				},
				_ = retry.tick(), if !acknowledged => {
					if let Some(value) = f(None).await? {
						return Ok(Some(value));
					}
				},
			}
		};
		let output = match response {
			Ok(output) if output.is_stored() => output,
			Ok(_) => return f(None).await,
			Err(error) => {
				if let Some(value) = f(None).await? {
					return Ok(Some(value));
				}
				return Err(error);
			},
		};
		drop(request);

		// The node may reach the store before its metadata reaches the index.
		let deadline = tokio::time::Instant::now() + config.index_timeout;
		loop {
			if let Some(value) = f(Some(output.clone())).await? {
				return Ok(Some(value));
			}
			let now = tokio::time::Instant::now();
			if now >= deadline {
				return Ok(None);
			}
			tokio::time::sleep(std::cmp::min(config.retry_interval, deadline - now)).await;
		}
	}
}
