use {crate::Cli, tangram_client::prelude::*};

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// The node.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_children(&mut self, mut args: Args) -> tg::Result<()> {
		// Get the node.
		args.locations.set_from_reference_if_unset(&args.reference);
		let reference = args.locations.apply_to_reference(&args.reference);
		let output = self
			.get_with_arg(&reference, tg::get::Arg::default())
			.await?;
		let node = output.referent.try_map(|node| match node {
			tg::get::Node::Id(id) => Ok(id),
			tg::get::Node::Pointer(_) => {
				Err(tg::error!(%reference, "the children node must be an ID"))
			},
		})?;

		// Get the children.
		let client = self.client().await?;
		let arg = tg::children::Arg { node };
		let output = client
			.children(arg)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to get the children"))?;
		let nodes = output
			.nodes
			.into_iter()
			.map(|node| node.node)
			.collect::<Vec<_>>();
		self.print_serde(nodes, args.print).await?;

		Ok(())
	}
}
