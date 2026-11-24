use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Get a sandbox.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[usage(long)]
	pub cached: bool,

	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	pub sandbox: tg::Referent<tg::sandbox::Id>,

	#[usage(flatten)]
	pub time_to_live: tangram_cli_reference::TimeToLive,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let location = args.locations.get_for_options(&args.sandbox);
	let id = args.sandbox.node;
	let entry = tg::sandbox::Options {
		location,
		tokens: args.sandbox.options.tokens,
		..Default::default()
	};
	let sandbox = tg::Sandbox::new(id.clone(), entry);
	let options = tg::sandbox::get::Options {
		cached: args.cached,
		time_to_live: args.time_to_live.get(),
	};
	let output = sandbox
		.try_get_with_instance(&client, options)
		.await
		.map_err(|error| tg::error!(!error, sandbox = %id, "failed to get the sandbox"))?
		.ok_or_else(|| tg::error!(sandbox = %id, "failed to find the sandbox"))?;
	cli.print_serde(output.as_ref(), args.print).await?;
	Ok(())
}
