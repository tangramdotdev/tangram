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
		let locations = args.locations;
		let print = args.print;

		// Get the reference.
		let referent = self.resolve(&args.reference).await?;
		let is_process = matches!(
			referent.node(),
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map::<tg::process::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let process = tg::Reference::with_node_and_tokens(
				tg::reference::Node::Id(process.node.into()),
				process.options.tokens,
			);
			let args = crate::process::stored::Args {
				locations,
				print,
				process,
			};
			self.command_process_stored(args).await?;
		} else {
			let object = referent
				.into_graph_edge()?
				.try_map::<tg::object::Id, _>(|edge| {
					edge.try_unwrap_object()
						.map(|object| object.id())
						.map_err(|_| tg::error!("expected an object"))
				})?;
			let object = tg::Reference::with_node_and_tokens(
				tg::reference::Node::Id(object.node.into()),
				object.options.tokens,
			);
			let args = crate::object::stored::Args {
				locations,
				object,
				print,
			};
			self.command_object_stored(args).await?;
		}

		Ok(())
	}
}
