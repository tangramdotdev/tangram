use {crate::Cli, tangram_client::prelude::*};

/// Display a tree for a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The maximum depth to render.
	#[arg(long)]
	pub depth: Option<u32>,

	/// Whether to view the item as a tag, package, or value.
	#[arg(long, default_value = "value")]
	pub kind: crate::view::Kind,

	/// The reference to display a tree for.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_tree(&mut self, args: Args) -> tg::Result<()> {
		let args = crate::view::Args {
			collapse_process_children: false,
			depth: args.depth,
			expand_objects: matches!(args.kind, crate::view::Kind::Value | crate::view::Kind::Tag),
			expand_packages: true,
			expand_processes: true,
			expand_tags: true,
			expand_metadata: false,
			kind: args.kind,
			mode: crate::view::Mode::Inline,
			reference: args.reference,
		};
		self.command_view(args).await?;
		Ok(())
	}
}
