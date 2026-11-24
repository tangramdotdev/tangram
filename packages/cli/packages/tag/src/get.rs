use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Get a tag.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[usage(long)]
	pub cached: bool,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	pub tag: tg::Referent<tg::tag::Selector>,

	#[usage(flatten)]
	pub time_to_live: tangram_cli_reference::TimeToLive,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::tag::get::Arg {
		cached: args.cached,
		location: args.location.get_for_options(&args.tag),
		time_to_live: args.time_to_live.get(),
		tokens: args.tag.options.tokens,
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
	cli.print_location_and_tokens(location.as_ref(), &tokens)?;
	cli.print_serde(data, args.print).await?;
	Ok(())
}
