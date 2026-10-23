use {crate::Component, tangram_client::prelude::*};

/// Get the time of the server's virtual clock.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Component<'_> {
	pub async fn command_clock_get(&mut self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.get_clock()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the clock"))?;
		self.print_serde(output, tangram_cli_core::print::Options::default())
			.await?;
		Ok(())
	}
}
