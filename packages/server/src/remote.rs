use {
	crate::{Server, Session},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

impl Session {
	pub async fn get_remote_session(&self, remote: String) -> tg::Result<tg::Session> {
		self.try_get_remote_session(remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub async fn try_get_remote_session(&self, remote: String) -> tg::Result<Option<tg::Session>> {
		let Some(output) = self
			.try_get_remote(&remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote"))?
		else {
			return Ok(None);
		};
		let client = self
			.server
			.get_or_create_remote_client(output.url)
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let mut context = client.context().clone();
		context.token = output.token;
		Ok(Some(client.session(&context)))
	}
}

impl Server {
	pub(crate) fn get_or_create_remote_client(&self, url: Uri) -> tg::Result<tg::Client> {
		if let Some(client) = self.remote_clients.get(&url) {
			return Ok(client.clone());
		}
		let client = self.create_remote_client(url.clone())?;
		self.remote_clients.insert(url, client.clone());
		Ok(client)
	}

	pub(crate) fn create_remote_client(&self, url: Uri) -> tg::Result<tg::Client> {
		tg::Client::new(tg::Arg {
			url: Some(url),
			version: Some(self.version.clone()),
			token: None,
			pool: None,
			reconnect: None,
			retry: None,
		})
	}
}
