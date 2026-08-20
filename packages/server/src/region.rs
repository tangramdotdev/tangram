use {
	crate::{Server, session::Session},
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) async fn get_primary_region_session(&self) -> tg::Result<tg::Session> {
		let region = self
			.server
			.config()
			.primary_region()
			.ok_or_else(|| tg::error!("the primary region is not configured"))?;
		self.get_region_session(region).await
	}

	pub(crate) async fn get_region_session(&self, region: &str) -> tg::Result<tg::Session> {
		self.verify_request_with_network_access()?;
		self.get_region_session_inner(region).await
	}

	pub(crate) async fn get_region_session_for_process(
		&self,
		region: &str,
	) -> tg::Result<tg::Session> {
		self.verify_request_can_access_remote_process()?;
		self.get_region_session_inner(region).await
	}

	async fn get_region_session_inner(&self, region: &str) -> tg::Result<tg::Session> {
		let client = self.server.get_region_client(region).await?;
		let context = client.context().clone();
		context.set_token(self.context.token.clone());
		let session = client.session(&context);
		Ok(session)
	}

	pub(crate) async fn forward_request_to_primary_region(
		&self,
		request: http::Request<tangram_http::body::Boxed>,
	) -> tg::Result<http::Response<tangram_http::body::Boxed>> {
		let region = self
			.server
			.config()
			.primary_region()
			.ok_or_else(|| tg::error!("the primary region is not configured"))?;
		let client = self.server.get_region_client(region).await?;
		let context = client.context().clone();
		context.set_token(self.context.token.clone());
		let session = client.session(&context);
		let response = session.send(request).await?;

		Ok(response)
	}
}

impl Server {
	#[must_use]
	pub fn is_primary_region(&self) -> bool {
		let config = self.config();
		config
			.primary_region()
			.is_none_or(|primary_region| config.region.as_deref() == Some(primary_region))
	}

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
