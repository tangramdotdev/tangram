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
		if !location.as_ref().is_some_and(tg::Location::is_remote) && self.user_token().is_none() {
			return Err(tg::error!("not logged in"));
		}
		let client = self.client().await?;
		let user = client
			.get_user(tg::user::get::Arg {
				location: location.map(Into::into),
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to get the user"))?
			.ok_or_else(|| tg::error!("not logged in"))?;
		self.print_serde(user, args.print).await?;
		Ok(())
	}
}
