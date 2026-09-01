use {crate::Cli, tangram_client::prelude::*};

/// List user tokens.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub output: crate::print::OutputOptions,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_token_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_user_tokens(tg::user::token::list::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to list the user tokens"))?;
		if args.output.verbose {
			self.print_serde(output, args.print).await?;
		} else {
			self.print_serde(output.data, args.print).await?;
		}

		Ok(())
	}
}
