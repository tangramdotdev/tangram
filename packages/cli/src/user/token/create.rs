use {crate::Cli, tangram_client::prelude::*};

/// Create a user token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_token_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.create_user_token(tg::user::token::create::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the user token"))?;
		self.print_serde(output.data, args.print).await?;

		Ok(())
	}
}
