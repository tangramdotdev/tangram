use {
	crate::{Config, Instance, Temp},
	futures::FutureExt as _,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

/// A server that runs in the test process.
pub struct Server {
	config: Config,
	owned: Option<tangram_server::Owned>,
	path: PathBuf,
	semaphores: Vec<String>,
	_temp: Temp,
	url: tangram_uri::Uri,
}

impl Server {
	/// Start a server in a new temporary directory.
	pub async fn start(mut config: Config) -> tg::Result<Self> {
		crate::instance::assign_indexer_id(&mut config);
		let semaphores = config.semaphore_names();
		crate::semaphore::record(&semaphores);
		Self::start_inner(config, None, semaphores).await
	}

	/// Start a server in an instance with a configuration from `Instance::config`. A server in a local instance uses the instance's directory.
	pub async fn start_with_instance(instance: &Instance, config: Config) -> tg::Result<Self> {
		let config = instance.server_config(config)?;
		let path = instance.directory().map(ToOwned::to_owned);
		Self::start_inner(config, path, Vec::new()).await
	}

	async fn start_inner(
		config: Config,
		path: Option<PathBuf>,
		semaphores: Vec<String>,
	) -> tg::Result<Self> {
		let temp = Temp::new()
			.map_err(|error| tg::error!(!error, "failed to create the temporary directory"))?;
		let path = path.unwrap_or_else(|| temp.path().join("server"));
		let url = crate::client::server_url(&config, &path)?;
		let owned = start(&config, &path).await?;
		let server = Self {
			config,
			owned: Some(owned),
			path,
			semaphores,
			_temp: temp,
			url,
		};

		Ok(server)
	}

	pub fn client(&self) -> tg::Result<tg::Client> {
		crate::client::client(&self.url)
	}

	/// Get the server as an instance in the test process, which acts with the server's own context.
	pub fn instance(&self) -> tg::Result<impl tg::Instance> {
		let owned = self
			.owned
			.as_ref()
			.ok_or_else(|| tg::error!("the server is not running"))?;

		Ok(owned.instance())
	}

	/// Get the server's directory.
	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Stop the server gracefully and start it again with the same directory.
	pub async fn restart(&mut self) -> tg::Result<()> {
		self.shutdown().await?;
		let owned = start(&self.config, &self.path).await?;
		self.owned = Some(owned);

		Ok(())
	}

	/// Stop the server gracefully and remove its directory.
	pub async fn stop(mut self) -> tg::Result<()> {
		self.shutdown().await?;

		Ok(())
	}

	async fn shutdown(&mut self) -> tg::Result<()> {
		let Some(owned) = self.owned.take() else {
			return Ok(());
		};
		owned.shutdown(tangram_server::Shutdown::Terminate);
		owned.wait().await?;

		Ok(())
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		// Signal the server to stop. The test process exits shortly after, so the server is not awaited.
		if let Some(owned) = self.owned.take() {
			owned.shutdown(tangram_server::Shutdown::Terminate);
		}
		crate::semaphore::unlink(&self.semaphores);
	}
}

async fn start(config: &Config, path: &Path) -> tg::Result<tangram_server::Owned> {
	let config = server_config(config, path)?;
	let owned = tangram_server::start(config).boxed().await?;

	Ok(owned)
}

/// Resolve the server configuration the way `tangram serve` does.
fn server_config(config: &Config, path: &Path) -> tg::Result<tangram_server::Config> {
	let cli = serde_json::from_value::<tangram_cli_core::config::Config>(config.value().clone())
		.map_err(|error| tg::error!(!error, "invalid server configuration"))?;
	let mut config = tangram_cli_server::config::resolve_server_config(&cli)?;
	config.directory = Some(path.to_owned());
	config.version = Some(tangram_cli_core::version());

	Ok(config)
}
