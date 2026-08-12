use {crate::Cli, tangram_client::prelude::*};

/// Get the current user's usage.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub period: crate::usage::PeriodArgs,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_usage(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let user = client
			.get_current_user(tg::user::current::Arg::default())
			.await?
			.ok_or_else(|| tg::error!("not logged in"))?;
		let selector = tg::user::Selector::Id(user.id);
		let usage = client
			.try_get_user_usage(&selector, args.period.into())
			.await?
			.ok_or_else(|| tg::error!("failed to find the user"))?;
		self.print_serde(usage, args.print).await?;

		Ok(())
	}
}
