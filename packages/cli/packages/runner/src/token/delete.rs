use {crate::Component, tangram_client::prelude::*};

/// Delete a runner token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub runner: tg::runner::Id,

	#[arg(index = 2)]
	pub token: tg::token::Id,
}

impl Component<'_> {
	pub async fn command_runner_token_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::runner::token::delete::Arg {
			location: args.location.get(),
		};
		client
			.try_delete_runner_token(
				&args.runner,
				&args.token,
				arg,
			)
			.await
			.map_err(|error| tg::error!(!error, runner = %args.runner, token = %args.token, "failed to delete the runner token"))?
			.ok_or_else(|| tg::error!(runner = %args.runner, token = %args.token, "failed to find the runner token"))?;

		Ok(())
	}
}
