use {crate::Cli, tangram_client as tg};

/// Display a tree for a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The maximum depth to render.
	#[arg(long)]
	pub depth: Option<u32>,

	#[arg(long, default_value = "value")]
	pub mode: crate::view::Mode,

	/// The reference to display a tree for.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_tree(&mut self, args: Args) -> tg::Result<()> {
		let args = crate::view::Args {
			collapse_process_children: false,
			depth: args.depth,
			expand_objects: matches!(args.mode, crate::view::Mode::Value | crate::view::Mode::Tag),
			expand_packages: true,
			expand_processes: true,
			expand_tags: true,
			kind: crate::view::Kind::Inline,
			mode: args.mode,
			reference: args.reference,
		};
		self.command_view(args).await?;
		Ok(())
	}
}
