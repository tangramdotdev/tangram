use {
	crate::Server,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

impl Server {
	pub async fn get_peer_clients(&self) -> tg::Result<BTreeMap<String, tg::Client>> {
		let peers = self
			.peers(None, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the peers"))?;
		let peers = peers
			.into_iter()
			.map(|peer| async {
				let client = self.get_peer_client(peer.clone()).await.map_err(
					|source| tg::error!(!source, peer = %peer, "failed to get the peer client"),
				)?;
				Ok::<_, tg::Error>((peer, client))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(peers)
	}

	pub async fn get_peer_client(&self, peer: String) -> tg::Result<tg::Client> {
		self.try_get_peer_client(peer)
			.await?
			.ok_or_else(|| tg::error!("failed to find the peer"))
	}

	pub async fn try_get_peer_client(&self, peer: String) -> tg::Result<Option<tg::Client>> {
		if let Some(client) = self.peers.get(&peer) {
			return Ok(Some(client.clone()));
		}
		let peer_config = self
			.config()
			.peers
			.as_ref()
			.and_then(|peers| peers.iter().find(|config| config.name == peer));
		let Some(peer_config) = peer_config else {
			return Ok(None);
		};
		let reconnect =
			peer_config
				.reconnect
				.clone()
				.map(|reconnect| tangram_futures::retry::Options {
					backoff: reconnect.backoff,
					jitter: reconnect.jitter,
					max_delay: reconnect.max_delay,
					max_retries: reconnect.max_retries,
				});
		let retry = peer_config
			.retry
			.clone()
			.map(|retry| tangram_futures::retry::Options {
				backoff: retry.backoff,
				jitter: retry.jitter,
				max_delay: retry.max_delay,
				max_retries: retry.max_retries,
			});
		let client = tg::Client::new(
			peer_config.url.clone(),
			Some(self.version.clone()),
			peer_config.token.clone(),
			None,
			reconnect,
			retry,
		);
		self.peers.insert(peer, client.clone());
		Ok(Some(client))
	}
}
