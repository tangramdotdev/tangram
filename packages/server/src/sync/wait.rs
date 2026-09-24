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
		if requests.is_empty() {
			return Ok(None);
		}

		let mut retry_local = false;
		loop {
			let mut outputs = Vec::new();
			requests.retain_mut(|request| match request.output() {
				Output::Pending => true,
				Output::Ready(result) => {
					retry_local = true;
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

			for output in outputs {
				if let Some(value) = f(Some(output)).await? {
					return Ok(Some(value));
				}
			}
			if retry_local {
				// A finished sync can make data available locally while other syncs remain pending.
				if let Some(value) = f(None).await? {
					return Ok(Some(value));
				}
				retry_local = false;
			}
			if requests.is_empty() {
				return Ok(None);
			}

			let changed = {
				let mut changes = requests
					.iter_mut()
					.enumerate()
					.map(|(index, request)| async move { (index, request.changed().await) })
					.collect::<FuturesUnordered<_>>();
				changes.next().await
			};
			if let Some((index, Err(source))) = changed {
				requests.remove(index);
				retry_local = true;
				tracing::trace!(error = %source, "a sync control request closed");
			}
		}
	}
}
