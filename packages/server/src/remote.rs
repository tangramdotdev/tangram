use {
	crate::Server,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client as tg,
};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

impl Server {
	pub async fn get_remote_clients(&self) -> tg::Result<BTreeMap<String, tg::Client>> {
		let output = self.list_remotes(tg::remote::list::Arg::default()).await?;
		let remotes = output
			.data
			.into_iter()
			.map(|output| async {
				let client = self.get_remote_client(output.name.clone()).await?;
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
		let Some(output) = self.try_get_remote(&remote).await? else {
			return Ok(None);
		};
		let client = tg::Client::new(output.url, Some(self.version.clone()));
		self.remotes.insert(remote, client.clone());
		Ok(Some(client))
	}
}
