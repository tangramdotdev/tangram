use {
	crate::{Cli, Mode, version},
	tangram_client::prelude::*,
};

impl Cli {
	pub(crate) async fn client(&mut self) -> tg::Result<tg::Client> {
		// If the client has already been created, then return it.
		if let Some(client) = self.client.clone() {
			return Ok(client);
		}

		let client = match self.args.mode {
			Mode::Auto => self.client_with_auto_mode().await?,
			Mode::Client => self.client_with_client_mode().await?,
		};

		if self.health.is_none() && client.process().is_none() {
			let arg = tg::health::Arg {
				fields: Some(vec!["diagnostics".to_owned()]),
			};
			let health = client
				.health(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the health"))?;
			self.health.replace(health);
		}
		if !self.args.quiet.get()
			&& let Some(diagnostics) = self
				.health
				.as_ref()
				.and_then(|health| health.diagnostics.clone())
		{
			for diagnostic in diagnostics {
				let diagnostic: tg::Diagnostic = diagnostic.try_into()?;
				let diagnostic = tg::Referent::with_item(diagnostic);
				self.print_diagnostic(diagnostic).await;
			}
		}

		// Set the client.
		self.client.replace(client.clone());

		Ok(client)
	}

	async fn client_with_client_mode(&mut self) -> tg::Result<tg::Client> {
		let client = self.create_client()?;
		client.connect().await.map_err(
			|source| tg::error!(!source, url = %client.url(), "failed to connect to the server"),
		)?;
		Ok(client)
	}

	async fn client_with_auto_mode(&mut self) -> tg::Result<tg::Client> {
		let client = self.create_client()?;
		let local = client.url().scheme() == Some("http+unix")
			|| matches!(client.url().host_raw(), Some("localhost" | "0.0.0.0"));
		match client.connect().await {
			Ok(()) => (),
			Err(source) => {
				if client.process().is_some() || !local {
					return Err(
						tg::error!(!source, url = %client.url(), "failed to connect to the server"),
					);
				}
				self.spawn_server(&client).await.map_err(
					|source| tg::error!(!source, url = %client.url(), "failed to start the server"),
				)?;
			},
		}

		if local && client.process().is_none() {
			let arg = tg::health::Arg {
				fields: Some(vec!["version".to_owned(), "diagnostics".to_owned()]),
			};
			let health = client
				.health(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the health"))?;
			let server_version = health.version.clone();
			self.health.replace(health);
			if server_version.is_some_and(|server_version| version() != server_version) {
				client.disconnect().await;
				self.stop_server().await?;
				self.spawn_server(&client).await.map_err(
					|source| tg::error!(!source, url = %client.url(), "failed to start the server"),
				)?;
			}
		}

		Ok(client)
	}

	pub(crate) fn create_client(&self) -> tg::Result<tg::Client> {
		// Get the url.
		let url = self
			.args
			.url
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.server.http.as_ref())
				.and_then(|config| config.listeners.first())
				.map(|listener| listener.url.clone()))
			.unwrap_or_else(|| {
				let path = self.directory_path().join("socket");
				let path = path.to_str().unwrap();
				tangram_uri::Uri::builder()
					.scheme("http+unix")
					.authority(path)
					.path("")
					.build()
					.unwrap()
			});

		// Get the token.
		let token = self.args.token.clone();

		// Get the reconnect options.
		let reconnect = self
			.config
			.as_ref()
			.and_then(|config| config.client.as_ref())
			.and_then(|client| client.reconnect.clone())
			.map(|reconnect| tangram_futures::retry::Options {
				backoff: reconnect.backoff,
				jitter: reconnect.jitter,
				max_delay: reconnect.max_delay,
				max_retries: reconnect.max_retries,
			});

		// Get the retry options.
		let retry = self
			.config
			.as_ref()
			.and_then(|config| config.client.as_ref())
			.and_then(|client| client.retry.clone())
			.map(|retry| tangram_futures::retry::Options {
				backoff: retry.backoff,
				jitter: retry.jitter,
				max_delay: retry.max_delay,
				max_retries: retry.max_retries,
			});

		// Get the process.
		let process = std::env::var("TANGRAM_PROCESS")
			.ok()
			.map(|value| {
				value
					.parse()
					.map_err(|source| tg::error!(!source, "failed to parse TANGRAM_PROCESS"))
			})
			.transpose()?;

		let arg = tg::Arg {
			url: Some(url),
			version: Some(version()),
			token,
			process,
			reconnect,
			retry,
		};

		let client = tg::Client::new(arg)?;

		Ok(client)
	}
}
