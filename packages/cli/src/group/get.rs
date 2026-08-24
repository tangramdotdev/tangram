use {crate::Cli, tangram_client::prelude::*};

/// Get a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[arg(index = 1)]
	pub group: tg::group::Selector,

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
	pub async fn command_group_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::get::Arg {
			cached: args.cached,
			location: args.location.get(),
			tokens: args.tokens,
			ttl: args.ttl.get(),
		};
		let group = client
			.try_get_group(&args.group, arg)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, "failed to get the group"))?
			.ok_or_else(|| tg::error!("failed to find the group"))?;
		let tg::group::get::Output {
			id,
			location,
			name,
			parent,
			specifier,
			tokens,
		} = group;
		self.print_location_and_tokens(location.as_ref(), &tokens)?;
		let data = tg::group::Data {
			id,
			name,
			parent,
			specifier,
		};
		self.print_serde(data, args.print).await?;
		Ok(())
	}
}
