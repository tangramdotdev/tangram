use crate::Cli;
use tangram_client as tg;

/// Run the server in the foreground.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_run(&self, _args: Args) -> tg::Result<()> {
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
		server.wait().await;

		Ok(())
	}
}
