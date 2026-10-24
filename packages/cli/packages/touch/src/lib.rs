use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Touch an object or a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

pub async fn command_touch(cli: &mut Cli, mut args: Args) -> tg::Result<()> {
	args.locations.set_from_reference_if_unset(&args.reference);
	let reference = args.locations.apply_to_reference(&args.reference);
	let locations = args.locations;

	let referent = tangram_cli_get::get_with_follow(cli, &reference).await?;
	match referent.node {
		tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
			let process = tg::Referent::new(id.try_into()?, referent.options);
			let entry = tangram_cli_process::touch::Options { locations };
			tangram_cli_process::touch::command_process_touch_inner(cli, process, entry).await?;
		},
		tg::get::Node::Id(id) => {
			let object = tg::Referent::new(id.try_into()?, referent.options);
			let options = tangram_cli_object::touch::Options { locations };
			tangram_cli_object::touch::command_object_touch_inner(cli, object, options).await?;
		},
		tg::get::Node::Pointer(_) => {
			return Err(tg::error!("expected an object or process id"));
		},
	}

	Ok(())
}
