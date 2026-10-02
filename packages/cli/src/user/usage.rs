use {crate::Cli, tangram_client::prelude::*};

/// Get the current user's usage.
///
/// Usage includes all regions of the selected server unless a specific region is selected with --location.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub period: crate::usage::PeriodArgs,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_usage(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let location = args.location.get();
		let arg = tg::user::current::Arg {
			location: location.clone(),
		};
		let user = client
			.get_current_user(arg)
			.await?
			.ok_or_else(|| tg::error!("not logged in"))?;
		let selector = tg::user::Selector::Id(user.data.id);
		let arg = tg::usage::Arg {
			location,
			..args.period.into()
		};
		let usage = client
			.try_get_user_usage(&selector, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to find the user"))?;
		self.print_serde(usage, args.print).await?;

		Ok(())
	}
}
