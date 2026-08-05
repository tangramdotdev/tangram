use {crate::Cli, tangram_client::prelude::*};

/// List runner tokens.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub runner: tg::runner::Id,
}

impl Cli {
	pub async fn command_runner_token_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_runner_tokens(&args.runner, tg::runner::token::list::Arg::default())
			.await
			.map_err(
				|error| tg::error!(!error, runner = %args.runner, "failed to list the runner tokens"),
			)?;
		self.print_serde(output.data, args.print).await?;

		Ok(())
	}
}
