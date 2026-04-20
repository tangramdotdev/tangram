use {crate::Server, std::collections::BTreeMap, tangram_client::prelude::*, tangram_uri::Uri};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

impl Server {
	pub async fn get_remote_clients(&self) -> tg::Result<BTreeMap<String, tg::Client>> {
		let remotes = self
			.remotes
			.iter()
			.map(|remote| (remote.key().clone(), remote.value().clone()))
			.collect();
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
		let client = self.create_remote_client(&remote, output.url);
		self.remotes.insert(remote, client.clone());
		Ok(Some(client))
	}

	pub(crate) fn create_remote_client(&self, remote: &str, url: Uri) -> tg::Client {
		let remote_config = self
			.config()
			.remotes
			.as_ref()
			.and_then(|remotes| remotes.iter().find(|r| r.name == remote));
		let token = remote_config.and_then(|r| r.token.clone());
		let reconnect = remote_config
			.and_then(|r| r.reconnect.clone())
			.map(|reconnect| tangram_futures::retry::Options {
				backoff: reconnect.backoff,
				jitter: reconnect.jitter,
				max_delay: reconnect.max_delay,
				max_retries: reconnect.max_retries,
			});
		let retry = remote_config.and_then(|r| r.retry.clone()).map(|retry| {
			tangram_futures::retry::Options {
				backoff: retry.backoff,
				jitter: retry.jitter,
				max_delay: retry.max_delay,
				max_retries: retry.max_retries,
			}
		});
		tg::Client::new(
			url,
			Some(self.version.clone()),
			token,
			None,
			reconnect,
			retry,
		)
	}
}
