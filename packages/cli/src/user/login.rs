use {crate::Cli, tangram_client::prelude::*};

/// Log in.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub email: Option<String>,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub parent: tg::Specifier,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_login(&mut self, args: Args) -> tg::Result<()> {
		let location = args.location.to_location()?;
		let client = self.client().await?;
		let arg = tg::user::login::Arg {
			parent: args.parent,
			email: args.email,
			location: location.clone().map(Into::into),
		};
		let output = client
			.login_user(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to log in"))?;
		if !location.as_ref().is_some_and(tg::Location::is_remote)
			&& !output
				.user
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			self.write_token(output.token)?;
		}
		self.print_serde(output.user, args.print).await?;
		Ok(())
	}
}
