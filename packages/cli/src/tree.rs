use crate::Cli;
use tangram_client as tg;

/// Display a tree for a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The maximum depth to render.
	#[arg(long)]
	pub depth: Option<u32>,

	/// If this flag is set, the lock will not be updated.
	#[arg(long, default_value = "false")]
	pub locked: bool,

	/// The reference to display a tree for.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_tree(&mut self, args: Args) -> tg::Result<()> {
		let args = crate::view::Args {
			depth: args.depth,
			kind: crate::view::Kind::Inline,
			locked: args.locked,
			reference: args.reference,
		};
		self.command_view(args).await?;
		Ok(())
	}
}
