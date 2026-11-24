use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a tag.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	pub pattern: tg::specifier::Pattern,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[usage(long)]
	pub recursive: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::tag::delete::Arg {
		location: args.location.get(),
		pattern: args.pattern.clone(),
		recursive: args.recursive,
	};
	let output = client
		.delete_tags(arg)
		.await
		.map_err(|error| tg::error!(!error, pattern = %args.pattern, "failed to delete the tag"))?;
	cli.print_serde(output, args.print).await?;
	Ok(())
}
