use {crate::Cli, tangram_client::prelude::*};

/// Manage the current user's billing payment method.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,
}

impl Cli {
	pub async fn command_user_billing_manage(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::user::billing::manage::Arg {
			location: args.location.get(),
		};
		let output = client
			.manage_user_billing(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to manage the user billing"))?;
		self.open_url(&output.url);

		Ok(())
	}
}
