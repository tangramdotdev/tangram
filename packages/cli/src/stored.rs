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
		let is_process = matches!(
			referent.node(),
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map::<tg::process::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let options = process.options.clone().into();
			let reference = tg::Reference::with_node_and_options(
				tg::reference::Node::Id(process.node.clone().into()),
				options,
			);
			let args = crate::process::stored::Args {
				locations: crate::location::Args::default(),
				print,
				process: reference,
			};
			self.command_process_stored_with_referent(args, process)
				.await?;
		} else {
			let object = referent
				.into_graph_edge()?
				.try_map::<tg::object::Id, _>(|edge| {
					edge.try_unwrap_object()
						.map(|object| object.id())
						.map_err(|_| tg::error!("expected an object"))
				})?;
			let options = object.options.clone().into();
			let reference = tg::Reference::with_node_and_options(
				tg::reference::Node::Id(object.node.clone().into()),
				options,
			);
			let args = crate::object::stored::Args {
				locations: crate::location::Args::default(),
				object: reference,
				print,
			};
			self.command_object_stored_with_referent(args, object)
				.await?;
		}

		Ok(())
	}
}
