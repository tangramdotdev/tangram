use {crate::Cli, tangram_client::prelude::*};

/// Get a process's or object's storage status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_stored(&mut self, args: Args) -> tg::Result<()> {
		let reference = args.locations.apply_to_reference(&args.reference);
		let print = args.print;

		// Get the reference.
		let referent = self.resolve(&reference).await?;
		match referent.node {
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				let options = crate::process::stored::Options {
					locations: crate::location::Args::default(),
					print,
				};
				self.command_process_stored_inner(process, options).await?;
			},
			node => {
				let object = tg::Referent::new(node, referent.options)
					.into_graph_edge()?
					.try_map::<tg::object::Id, _>(|edge| {
						edge.try_unwrap_object()
							.map(|object| object.id())
							.map_err(|_| tg::error!("expected an object"))
					})?;
				let options = crate::object::stored::Options {
					locations: crate::location::Args::default(),
					print,
				};
				self.command_object_stored_inner(object, options).await?;
			},
		}

		Ok(())
	}
}
