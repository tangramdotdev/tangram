use {crate::Cli, tangram_client::prelude::*};

/// Log out.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,
}

impl Cli {
	pub async fn command_user_logout(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let location = args.location.get();
		let arg = tg::user::current::Arg {
			location: location.clone(),
		};
		let Some(user) = client.get_current_user(arg).await? else {
			return Ok(());
		};
		let location = location.or_else(|| user.location.map(Into::into));
		let local = location
			.as_ref()
			.and_then(tg::location::Arg::to_location)
			.is_some_and(|location| location.is_local());
		let arg = tg::user::logout::Arg { location };
		client
			.logout(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to log out"))?;
		if local {
			self.delete_token()?;
		}
		Ok(())
	}
}
