use {
	crate::{
		Session,
		sync::control::{Client, Output},
	},
	futures::{StreamExt as _, stream::FuturesUnordered},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) async fn try_get_with_sync_wait<T, F, Fut>(
		&self,
		tokens: &tg::authorization::Tokens,
		arg: tg::sync::control::ClientRequestArg,
		mut f: F,
	) -> tg::Result<Option<T>>
	where
		F: FnMut(Option<tg::sync::control::GetServerResponseOutput>) -> Fut,
		Fut: Future<Output = tg::Result<Option<T>>>,
	{
		let deadline = tokio::time::Instant::now() + self.server.config.sync.control.index_timeout;
		self.try_get_with_sync_wait_until(tokens, arg, deadline, &mut f)
			.await
	}

	pub(crate) async fn try_get_with_sync_wait_until<T, F, Fut>(
		&self,
		tokens: &tg::authorization::Tokens,
		arg: tg::sync::control::ClientRequestArg,
		deadline: tokio::time::Instant,
		mut f: F,
	) -> tg::Result<Option<T>>
	where
		F: FnMut(Option<tg::sync::control::GetServerResponseOutput>) -> Fut,
		Fut: Future<Output = tg::Result<Option<T>>>,
	{
		let future = async {
			let config = &self.server.config.sync.control;
			if let Some(value) = f(None).await? {
				return Ok(Some(value));
			}
			let mut ids = BTreeSet::new();
			let tokens = tokens
				.local_authorization()
				.iter()
				.filter_map(|token| self.try_get_sync_id_from_token(token))
				.filter(|id| ids.insert(id.clone()))
				.collect::<Vec<_>>();
			if arg.node().is_none() {
				return Err(tg::error!("expected a sync node request"));
			}
			let client = self
				.sync_control
				.clone()
				.unwrap_or_else(|| Arc::new(Client::default()));
			let mut requests = tokens
				.into_iter()
				.map(|id| client.request(self, &id, arg.clone()))
				.collect::<Vec<_>>();
			let options = config.index_retry.clone().into();
			let mut retry = std::pin::pin!(tangram_futures::retry::stream(options));
			retry.next().await;
			let mut outputs = Vec::new();
			let mut retry_local = false;

			loop {
				requests.retain_mut(|request| match request.output() {
					Output::Pending => true,
					Output::Ready(result) => {
						match result {
							Ok(Some(output)) => {
								outputs.push(output);
							},
							Ok(None) => {},
							Err(error) => tracing::trace!(%error, "a sync control request failed"),
						}
						false
					},
				});

				// A control proof can precede the index, and another sync may supply a usable proof first.
				for output in &outputs {
					if let Some(value) = f(Some(output.clone())).await? {
						return Ok(Some(value));
					}
				}
				if retry_local && let Some(value) = f(None).await? {
					return Ok(Some(value));
				}
				retry_local = true;
				if tokio::time::Instant::now() >= deadline {
					return Ok(None);
				}

				let changed = {
					let mut changes = requests
						.iter_mut()
						.enumerate()
						.map(|(index, request)| async move { (index, request.changed().await) })
						.collect::<FuturesUnordered<_>>();
					tokio::select! {
						changed = changes.next(), if !changes.is_empty() => changed,
						tick = retry.next() => {
							if tick.is_none() {
								return Ok(None);
							}
							None
						},
						() = tokio::time::sleep_until(deadline) => None,
					}
				};
				if let Some((index, Err(source))) = changed {
					requests.remove(index);
					tracing::trace!(error = %source, "a sync control request closed");
				}
			}
		};
		let output = tokio::time::timeout_at(deadline, future).await;
		output.unwrap_or(Ok(None))
	}
}
