use crate::Cli;
use tangram_client as tg;

/// Run the server in the foreground.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_server_run(&self, _args: Args) -> tg::Result<()> {
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

		Ok(())
	}
}
