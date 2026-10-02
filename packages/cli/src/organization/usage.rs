use {crate::Cli, tangram_client::prelude::*};

/// Get an organization's usage.
///
/// Usage includes all regions of the selected server unless a specific region is selected with --location.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub organization: tg::Referent<tg::organization::Selector>,

	#[command(flatten)]
	pub period: crate::usage::PeriodArgs,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_organization_usage(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let mut arg = tg::usage::Arg::from(args.period);
		arg.location = args.location.get_for_options(&args.organization);
		arg.tokens = args.organization.options.tokens;
		let usage = client
			.try_get_organization_usage(&args.organization.node, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to find the organization"))?;
		self.print_serde(usage, args.print).await?;

		Ok(())
	}
}
