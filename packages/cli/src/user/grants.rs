use {crate::Cli, tangram_client::prelude::*};

/// List namespace grants for a user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub user: String,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_grants(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_user_namespace_grants(&args.user, tg::user::grants::Arg::default())
			.await
			.map_err(
				|error| tg::error!(!error, user = %args.user, "failed to list the namespace grants"),
			)?
			.ok_or_else(|| tg::error!(user = %args.user, "failed to find the user"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
