use {crate::Component, tangram_client::prelude::*};

/// Create a runner.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(long)]
	pub owner: Option<tg::principal::Selector>,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

impl Component<'_> {
	pub async fn command_runner_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::runner::create::Arg {
			location: args.location.get(),
			owner: args.owner,
		};
		let output = client
			.create_runner(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the runner"))?;
		self.print_serde(output, args.print).await?;

		Ok(())
	}
}
