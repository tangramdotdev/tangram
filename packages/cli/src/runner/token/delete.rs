use {crate::Cli, tangram_client::prelude::*};

/// Delete a runner token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub runner: tg::runner::Id,

	#[arg(index = 2)]
	pub token: tg::token::Id,
}

impl Cli {
	pub async fn command_runner_token_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.try_delete_runner_token(
				&args.runner,
				&args.token,
				tg::runner::token::delete::Arg::default(),
			)
			.await
			.map_err(|error| tg::error!(!error, runner = %args.runner, token = %args.token, "failed to delete the runner token"))?
			.ok_or_else(|| tg::error!(runner = %args.runner, token = %args.token, "failed to find the runner token"))?;

		Ok(())
	}
}
