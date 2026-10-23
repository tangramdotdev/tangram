use {crate::Component, tangram_client::prelude::*};

/// Get a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub tag: tg::Referent<tg::tag::Selector>,

	#[command(flatten)]
	pub ttl: tangram_cli_get::Ttl,
}

impl Component<'_> {
	pub async fn command_tag_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::tag::get::Arg {
			cached: args.cached,
			location: args.location.get_for_options(&args.tag),
			tokens: args.tag.options.tokens,
			ttl: args.ttl.get(),
		};
		let tag = client
			.try_get_tag(&args.tag.node, arg)
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag.node, "failed to get the tag"))?
			.ok_or_else(|| tg::error!("failed to find the tag"))?;
		let tg::tag::get::Output {
			data,
			location,
			tokens,
		} = tag;
		self.print_location_and_tokens(location.as_ref(), &tokens)?;
		self.print_serde(data, args.print).await?;
		Ok(())
	}
}
