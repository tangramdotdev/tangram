use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get an object's availability.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let mut options = args.options;
	options
		.locations
		.set_from_reference_if_unset(&args.reference);
	let object =
		tangram_cli_reference::get_object_with_locations(cli, &args.reference, &options.locations)
			.await?;
	run_inner(cli, object, options).await
}

pub async fn run_inner(
	cli: &mut Cli,
	object: tg::Referent<tg::object::Id>,
	options: Options,
) -> tg::Result<()> {
	let client = cli.client().await?;
	let id = object.node.clone();
	let location = options.locations.get_for_options(&object);
	let object = tg::Object::with_referent(object);
	let options_ = tg::object::availability::Options { location };
	let output = object
		.availability_with_instance(&client, options_)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to get the object's availability"))?;
	cli.print_serde(output, options.print).await?;
	Ok(())
}
