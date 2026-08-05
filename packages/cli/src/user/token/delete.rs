use {crate::Cli, tangram_client::prelude::*};

/// Delete a user token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub token: tg::token::Id,
}

impl Cli {
	pub async fn command_user_token_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.try_delete_user_token(&args.token, tg::user::token::delete::Arg::default())
			.await
			.map_err(
				|error| tg::error!(!error, token = %args.token, "failed to delete the user token"),
			)?
			.ok_or_else(|| tg::error!(token = %args.token, "failed to find the user token"))?;

		Ok(())
	}
}
