use {
	futures::{StreamExt as _, TryStreamExt as _},
	std::time::Duration,
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
	usage_rs as usage,
};

/// Get a process's children.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub options: Options,

	pub reference: tg::Reference,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Options {
	#[usage(long)]
	pub length: Option<u64>,

	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(long)]
	pub position: Option<crate::stdio::read::Position>,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[usage(long)]
	pub size: Option<u64>,

	#[usage(flatten)]
	pub timeout: Timeout,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Timeout {
	#[usage(long = "timeout", overrides("--no-timeout"))]
	pub timeout: Option<humantime::Duration>,

	#[usage(long = "no-timeout", overrides("--timeout"))]
	pub no_timeout: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let mut options = args.options;
	options
		.locations
		.set_from_reference_if_unset(&args.reference);
	let process =
		tangram_cli_reference::get_process_with_locations(cli, &args.reference, &options.locations)
			.await?;
	run_inner(cli, process, options).await
}

pub(crate) async fn run_inner(
	cli: &mut Cli,
	process: tg::Referent<tg::process::Id>,
	options: Options,
) -> tg::Result<()> {
	let client = cli.client().await?;
	let id = process.node.clone();
	let location = options.locations.get_for_options(&process);
	let process = tg::Process::<tg::Value>::with_referent(process);
	let options_ = tg::process::children::get::Options {
		length: options.length,
		location,
		position: options.position.map(|position| position.0),
		size: options.size,
		timeout: options.timeout.get(),
	};
	let tokens = options.print.tokens.get();
	let stream = process
		.children_with_instance(&client, options_)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to get the process children"))?
		.map_ok(move |child| {
			let child = child.to_data();
			if tokens {
				child
			} else {
				child.without_location_and_tokens()
			}
		});
	cli.print_serde_stream(stream.boxed(), options.print)
		.await?;
	Ok(())
}

impl Timeout {
	fn get(&self) -> Option<Duration> {
		if self.no_timeout {
			None
		} else {
			self.timeout.map(Into::into).or(Some(Duration::ZERO))
		}
	}
}
