use {
	crate::{Server, Session},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

#[derive(Clone)]
pub(crate) struct Remote {
	pub name: String,
	pub url: Uri,
	pub token: Option<String>,
}

impl Session {
	pub async fn get_remote_session(&self, remote: String) -> tg::Result<tg::Session> {
		let client = self.get_remote_client(remote).await?;
		Ok(client.session(client.context()))
	}

	pub async fn get_remote_client(&self, remote: String) -> tg::Result<tg::Client> {
		self.try_get_remote_client(remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub async fn try_get_remote_client(&self, remote: String) -> tg::Result<Option<tg::Client>> {
		if let Some(client) = self.server.remotes.get(&remote) {
			return Ok(Some(client.clone()));
		}
		let Some(output) = self
			.try_get_remote_config(&remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote"))?
		else {
			return Ok(None);
		};
		let client =
			self.server
				.create_remote_client(&output.name, output.url.clone(), output.token)?;
		self.server.remotes.insert(output.name, client.clone());
		Ok(Some(client))
	}
}

impl Server {
	pub(crate) fn create_remote_client(
		&self,
		remote: &str,
		url: Uri,
		token: Option<String>,
	) -> tg::Result<tg::Client> {
		let remote_config = self
			.config()
			.remotes
			.as_ref()
			.and_then(|remotes| remotes.iter().find(|r| r.name == remote));
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
		tg::Client::new(tg::Arg {
			url: Some(url),
			version: Some(self.version.clone()),
			token,
			process: None,
			reconnect,
			retry,
		})
	}
}
