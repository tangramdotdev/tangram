use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Manage the server.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Health(HealthArgs),
	Run(RunArgs),
	Start(StartArgs),
	Stop(StopArgs),
}

/// Get the server's health.
#[derive(Debug, clap::Args)]
pub struct HealthArgs {}

/// Start the server.
#[derive(Debug, clap::Args)]
pub struct StartArgs {}

/// Stop the server.
#[derive(Debug, clap::Args)]
pub struct StopArgs {}

/// Run the server in the foreground.
#[derive(Debug, clap::Args)]
pub struct RunArgs {}

impl Cli {
	pub async fn command_server(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Health(_) => {
				let health = self.handle.health().await?;
				let health = serde_json::to_string_pretty(&health)
					.map_err(|source| tg::error!(!source, "failed to serialize"))?;
				println!("{health}");
			},
			Command::Start(_) => {
				Self::start_server(self.config.as_ref()).await?;
			},
			Command::Stop(_) => {
				self.handle.stop().await?;
			},
			Command::Run(_) => {
				let server = self.handle.as_ref().right().unwrap();

				// Stop the server if an interrupt signal is received.
				tokio::spawn({
					let server = server.clone();
					async move {
						tokio::signal::ctrl_c().await.unwrap();
						server.stop();
						tokio::signal::ctrl_c().await.unwrap();
						std::process::exit(130);
					}
				});

				// Join the server.
				server
					.join()
					.await
					.map_err(|source| tg::error!(!source, "failed to join the server"))?;
			},
		}
		Ok(())
	}
}
