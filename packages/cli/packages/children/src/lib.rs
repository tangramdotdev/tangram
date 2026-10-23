use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Fetch all pages.
	#[arg(long)]
	pub all: bool,

	/// Continue from this cursor.
	#[arg(long)]
	pub cursor: Option<String>,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[arg(long)]
	pub limit: Option<u64>,

	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// The node.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
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
		let arg = tg::children::Arg {
			cursor: args.cursor,
			limit: args.limit,
			node,
		};
		let output = if args.all {
			client.children_all(arg).await
		} else {
			client.children(arg).await
		}
		.map_err(|error| tg::error!(!error, %reference, "failed to get the children"))?;
		if args.output.verbose {
			self.print_serde(output, args.print).await?;
		} else {
			let nodes = output
				.data
				.into_iter()
				.map(|node| node.node)
				.collect::<Vec<_>>();
			self.print_serde(nodes, args.print).await?;
			if let Some(cursor) = output.cursor {
				self.print_info_message(&format!("Next cursor: {cursor}"));
			}
		}

		Ok(())
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
