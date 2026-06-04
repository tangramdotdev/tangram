use {crate::Cli, tangram_client::prelude::*};

/// Get a user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub user: tg::user::Selector,
}

impl Cli {
	pub async fn command_user_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::user::get::Arg {
			location: args.location.get(),
		};
		let user = client
			.try_get_user(&args.user, arg)
			.await
			.map_err(|error| tg::error!(!error, user = %args.user, "failed to get the user"))?
			.ok_or_else(|| tg::error!(user = %args.user, "failed to find the user"))?;
		self.print_serde(user, args.print).await?;
		Ok(())
	}
}
