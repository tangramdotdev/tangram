use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*, tangram_uri::Uri};

/// Run the server in the foreground.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The path to the config file.
	#[arg(env = "TANGRAM_CONFIG", long, short)]
	pub config: Option<PathBuf>,

	/// Override the `directory` key in the config.
	#[arg(env = "TANGRAM_DIRECTORY", long, short)]
	pub directory: Option<PathBuf>,

	/// Override the `remotes` key in the config.
	#[arg(long, conflicts_with = "remotes")]
	pub no_remotes: bool,

	/// Override the `remotes` key in the config.
	#[arg(long, short, value_delimiter = ',', conflicts_with = "no_remotes")]
	pub remotes: Option<Vec<String>>,

	/// The token.
	#[arg(env = "TANGRAM_TOKEN")]
	pub token: Option<String>,

	/// Override the tracing filter.
	#[arg(env = "TANGRAM_TRACING", long)]
	pub tracing: Option<String>,

	/// Override the `url` key in the config.
	#[arg(env = "TANGRAM_URL", long, short)]
	pub url: Option<Uri>,
}

impl Cli {
	pub async fn command_server_run(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the server.
		let server = handle.as_ref().unwrap_right();

		// Spawn a task to stop the server on the first interrupt signal and exit the process on the second.
		tokio::spawn({
			let server = server.clone();
			async move {
				tokio::signal::ctrl_c().await.unwrap();
				server.stop();
				drop(server);
				tokio::signal::ctrl_c().await.unwrap();
				std::process::exit(130);
			}
		});

		// Wait for the server.
		server.wait().await.unwrap();

		Ok(())
	}
}
