use {crate::Cli, tangram_client::prelude::*};

/// List grants for a user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 2)]
	pub parent: Option<tg::Specifier>,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub user: tg::user::Selector,
}

impl Cli {
	pub async fn command_user_grants(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::user::grants::Arg {
			location: args.location.get(),
			parent: args.parent.clone(),
		};
		let output = client
			.try_get_user_grants(&args.user, arg)
			.await
			.map_err(|error| tg::error!(!error, user = %args.user, "failed to list the grants"))?
			.ok_or_else(|| tg::error!(user = %args.user, "failed to find the user"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
