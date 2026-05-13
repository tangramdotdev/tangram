use {crate::Cli, tangram_client::prelude::*};

/// Get the current user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_whoami(&mut self, args: Args) -> tg::Result<()> {
		let location = args.location.to_location()?;
		let client = self.client().await?;
		let user = client
			.get_current_user(tg::user::current::Arg {
				location: location.map(Into::into),
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to get the user"))?
			.ok_or_else(|| tg::error!("not logged in"))?;
		self.print_serde(user, args.print).await?;
		Ok(())
	}
}
