use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn command_metadata(cli: &mut Cli, mut args: Args) -> tg::Result<()> {
	args.locations.set_from_reference_if_unset(&args.reference);
	let reference = args.locations.apply_to_reference(&args.reference);
	let locations = args.locations;
	let print = args.print;

	// Get the reference.
	let referent = tangram_cli_get::get_with_follow(cli, &reference).await?;
	match referent.node {
		tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
			let process = tg::Referent::new(id.try_into()?, referent.options);
			let options = tangram_cli_process::metadata::Options { locations, print };
			tangram_cli_process::metadata::command_process_metadata_inner(cli, process, options)
				.await?;
		},
		node => {
			let object = tg::Referent::new(node, referent.options)
				.into_graph_edge()?
				.try_map::<tg::object::Id, _>(|edge| {
					edge.try_unwrap_object()
						.map(|object| object.id())
						.map_err(|_| tg::error!("expected an object"))
				})?;
			let options = tangram_cli_object::metadata::Options { locations, print };
			tangram_cli_object::metadata::command_object_metadata_inner(cli, object, options)
				.await?;
		},
	}

	Ok(())
}
