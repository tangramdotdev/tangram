use {crate::Cli, tangram_client::prelude::*};

/// Get a user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(skip)]
	pub tokens: tg::authorization::Tokens,

	#[command(flatten)]
	pub ttl: crate::get::Ttl,

	#[arg(index = 1)]
	pub user: tg::user::Selector,
}

impl Cli {
	pub async fn command_user_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::user::get::Arg {
			cached: args.cached,
			location: args.location.get(),
			tokens: args.tokens,
			ttl: args.ttl.get(),
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
