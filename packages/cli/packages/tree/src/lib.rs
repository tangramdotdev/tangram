use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Display a tree for a node.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub alternate_screen: tangram_cli_view::AlternateScreen,

	/// The maximum depth to render, in levels below the root. Zero renders only the root.
	#[arg(long)]
	pub depth: Option<u32>,

	/// The reference to display a tree for.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_tree(&mut self, args: Args) -> tg::Result<()> {
		let args = tangram_cli_view::Args {
			alternate_screen: args.alternate_screen,
			collapse_process_children: false,
			depth: args.depth,
			expand_groups: true,
			expand_metadata: false,
			expand_objects: true,
			expand_organizations: true,
			expand_processes: true,
			expand_sandboxes: true,
			expand_tags: true,
			expand_users: true,
			mode: tangram_cli_view::Mode::Inline,
			reference: args.reference,
		};
		tangram_cli_view::Component::new(self)
			.command_view(args)
			.await?;
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
