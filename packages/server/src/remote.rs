use {
	crate::Server,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

impl Server {
	pub async fn get_remote_clients(&self) -> tg::Result<BTreeMap<String, tg::Client>> {
		let output = self
			.list_remotes(tg::remote::list::Arg::default())
			.await
			.map_err(|source| tg::error!(!source, "failed to list the remotes"))?;
		let remotes = output
			.data
			.into_iter()
			.map(|output| async {
				let name = output.name.clone();
				let client = self.get_remote_client(output.name.clone()).await.map_err(
					|source| tg::error!(!source, remote = %name, "failed to get the remote client"),
				)?;
				Ok::<_, tg::Error>((output.name, client))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(remotes)
	}

	pub async fn get_remote_client(&self, remote: String) -> tg::Result<tg::Client> {
		self.try_get_remote_client(remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub async fn try_get_remote_client(&self, remote: String) -> tg::Result<Option<tg::Client>> {
		if let Some(client) = self.remotes.get(&remote) {
			return Ok(Some(client.clone()));
		}
		let Some(output) = self
			.try_get_remote(&remote)
			.await
			.map_err(|source| tg::error!(!source, %remote, "failed to get the remote"))?
		else {
			return Ok(None);
		};
		let token = self.config().remotes.as_ref().and_then(|remotes| {
			let remote = remotes.iter().find(|r| r.name == remote)?;
			remote.token.clone()
		});
		let reconnect = self
			.config()
			.client
			.as_ref()
			.and_then(|client| client.reconnect.clone())
			.map(|reconnect| tangram_futures::retry::Options {
				backoff: reconnect.backoff,
				jitter: reconnect.jitter,
				max_delay: reconnect.max_delay,
				max_retries: reconnect.max_retries,
			});
		let client = tg::Client::new(output.url, Some(self.version.clone()), token, reconnect);
		self.remotes.insert(remote, client.clone());
		Ok(Some(client))
	}
}
