use {crate::Cli, tangram_client::prelude::*};

/// Get an organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[arg(index = 1)]
	pub organization: tg::organization::Selector,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(skip)]
	pub tokens: tg::authorization::Tokens,

	#[command(flatten)]
	pub ttl: crate::get::Ttl,
}

impl Cli {
	pub async fn command_organization_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::organization::get::Arg {
			cached: args.cached,
			location: args.location.get(),
			tokens: args.tokens,
			ttl: args.ttl.get(),
		};
		let organization = client
			.try_get_organization(&args.organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, organization = %args.organization, "failed to get the organization"),
			)?
			.ok_or_else(|| tg::error!("failed to find the organization"))?;
		self.print_serde(organization, args.print).await?;
		Ok(())
	}
}
