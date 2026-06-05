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
	pub async fn command_metadata(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;

		// Get the reference.
		let referent = self.get_resolved_reference(&args.reference).await?;
		match referent.item {
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process => {
				let args = crate::process::metadata::Args {
					locations,
					print,
					process: id.try_into()?,
				};
				self.command_process_metadata(args).await?;
			},
			item => {
				let edge = crate::get::get_item_to_graph_edge(item)?;
				let object = edge
					.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?
					.id();
				let args = crate::object::metadata::Args {
					locations: locations.clone(),
					object,
					print,
				};
				self.command_object_metadata(args).await?;
			},
		}

		Ok(())
	}
}
