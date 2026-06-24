use {
	crate::{Server, session::Session},
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) async fn get_region_session(&self, region: &str) -> tg::Result<tg::Session> {
		let _token = self
			.context
			.principal
			.to_id()
			.and_then(|id| tg::process::Id::try_from(id).ok());
		let _client = self.server.get_region_client(region).await?;
		todo!("propagate process authentication to regions")
	}
}

impl Server {
	pub async fn get_region_client(&self, region: &str) -> tg::Result<tg::Client> {
		self.try_get_region_client(region)
			.await?
			.ok_or_else(|| tg::error!("failed to find the region"))
	}

	pub async fn try_get_region_client(&self, region: &str) -> tg::Result<Option<tg::Client>> {
		if let Some(client) = self.regions.get(region) {
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
			token: None,
			pool: None,
			reconnect,
			retry,
			sync: tg::sync::Config {
				max_frame_size: self.config().sync.max_frame_size,
			},
		})?;
		self.regions.insert(region.to_owned(), client.clone());
		Ok(Some(client))
	}
}
