use {
	crate::Server,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

impl Server {
	pub async fn get_region_clients(&self) -> tg::Result<BTreeMap<String, tg::Client>> {
		let regions = self
			.config()
			.regions
			.as_ref()
			.map_or_else(Vec::new, |regions| {
				regions.iter().map(|region| region.name.clone()).collect()
			});
		let regions = regions
			.into_iter()
			.map(|region| async {
				let client = self.get_region_client(region.clone()).await.map_err(
					|source| tg::error!(!source, region = %region, "failed to get the region client"),
				)?;
				Ok::<_, tg::Error>((region, client))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(regions)
	}

	pub async fn get_region_client(&self, region: String) -> tg::Result<tg::Client> {
		self.try_get_region_client(region)
			.await?
			.ok_or_else(|| tg::error!("failed to find the region"))
	}

	pub async fn try_get_region_client(&self, region: String) -> tg::Result<Option<tg::Client>> {
		if let Some(client) = self.regions.get(&region) {
			return Ok(Some(client.clone()));
		}
		let region_config = self
			.config()
			.regions
			.as_ref()
			.and_then(|regions| regions.iter().find(|config| config.name == region));
		let Some(region_config) = region_config else {
			return Ok(None);
		};
		let reconnect =
			region_config
				.reconnect
				.clone()
				.map(|reconnect| tangram_futures::retry::Options {
					backoff: reconnect.backoff,
					jitter: reconnect.jitter,
					max_delay: reconnect.max_delay,
					max_retries: reconnect.max_retries,
				});
		let retry = region_config
			.retry
			.clone()
			.map(|retry| tangram_futures::retry::Options {
				backoff: retry.backoff,
				jitter: retry.jitter,
				max_delay: retry.max_delay,
				max_retries: retry.max_retries,
			});
		let client = tg::Client::new(tg::Arg {
			url: Some(region_config.url.clone()),
			version: Some(self.version.clone()),
			token: region_config.token.clone(),
			process: None,
			reconnect,
			retry,
		})?;
		self.regions.insert(region, client.clone());
		Ok(Some(client))
	}
}
