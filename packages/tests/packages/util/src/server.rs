use {
	crate::{Config, Temp},
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

/// A server that runs in the test process.
pub struct Server {
	config: Config,
	owned: Option<tangram_server::Owned>,
	temp: Temp,
}

impl Server {
	/// Start a server in a new temporary directory.
	pub async fn start(config: Config) -> tg::Result<Self> {
		crate::semaphore::record(&config.semaphore_names());
		let temp = Temp::new()?;
		let owned = start(&config, &directory(&temp)).await?;
		let server = Self {
			config,
			owned: Some(owned),
			temp,
		};

		Ok(server)
	}

	pub fn client(&self) -> tg::Result<tg::Client> {
		crate::client::client(&self.path())
	}

	/// Get the server's directory.
	#[must_use]
	pub fn path(&self) -> PathBuf {
		directory(&self.temp)
	}

	/// Stop the server gracefully and start it again with the same directory.
	pub async fn restart(&mut self) -> tg::Result<()> {
		self.shutdown().await?;
		let owned = start(&self.config, &self.path()).await?;
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
		crate::semaphore::unlink(&self.config.semaphore_names());
	}
}

fn directory(temp: &Temp) -> PathBuf {
	temp.path().join("server")
}

/// Resolve the server configuration the way `tangram serve` does, then start the server.
async fn start(config: &Config, path: &Path) -> tg::Result<tangram_server::Owned> {
	let cli = serde_json::from_value::<tangram_cli_core::config::Config>(config.value().clone())
		.map_err(|error| tg::error!(!error, "invalid server configuration"))?;
	let mut server = tangram_cli_server::config::resolve_server_config(&cli)?;
	server.directory = Some(path.to_owned());
	server.version = Some(tangram_cli_core::version());
	let owned = tangram_server::start(server).await?;

	Ok(owned)
}
