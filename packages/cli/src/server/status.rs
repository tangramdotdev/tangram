use {crate::Cli, tangram_client as tg};

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_server_status(&mut self, args: Args) -> tg::Result<()> {
		let status = if self.client().await.is_ok() {
			"started"
		} else {
			"stopped"
		};
		self.print_serde(status, args.print).await?;
		Ok(())
	}
}
