use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Touch an object.
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
}

pub async fn command_object_touch(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let mut options = args.options;
	options
		.locations
		.set_from_reference_if_unset(&args.reference);
	let object =
		tangram_cli_get::get_object_with_locations(cli, &args.reference, &options.locations)
			.await?;
	command_object_touch_inner(cli, object, options).await
}

pub async fn command_object_touch_inner(
	cli: &mut Cli,
	object: tg::Referent<tg::object::Id>,
	options: Options,
) -> tg::Result<()> {
	let client = cli.client().await?;
	let id = object.node.clone();
	let location = options.locations.get_for_options(&object);
	let object = tg::Object::with_referent(object);
	let options = tg::object::touch::Options { location };
	object
		.touch_with_instance(&client, options)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to touch the object"))?;
	Ok(())
}
