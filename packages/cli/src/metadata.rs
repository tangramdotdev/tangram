use {crate::Cli, tangram_client::prelude::*};

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_metadata(&mut self, mut args: Args) -> tg::Result<()> {
		args.locations.set_from_reference_if_unset(&args.reference);
		let reference = args.locations.apply_to_reference(&args.reference);
		let locations = args.locations;
		let print = args.print;

		// Get the reference.
		let referent = self.get_with_follow(&reference).await?;
		match referent.node {
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				let options = crate::process::metadata::Options { locations, print };
				self.command_process_metadata_inner(process, options)
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
				let options = crate::object::metadata::Options { locations, print };
				self.command_object_metadata_inner(object, options).await?;
			},
		}

		Ok(())
	}
}
