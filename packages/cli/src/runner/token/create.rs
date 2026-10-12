use {crate::Cli, tangram_client::prelude::*};

/// Create a runner token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub runner: tg::runner::Id,
}

impl Cli {
	pub async fn command_runner_token_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::runner::token::create::Arg {
			location: args.location.get(),
		};
		let output = client
			.create_runner_token(&args.runner, arg)
			.await
			.map_err(
				|error| tg::error!(!error, runner = %args.runner, "failed to create a runner token"),
			)?;
		self.print_serde(output.data, args.print).await?;

		Ok(())
	}
}
