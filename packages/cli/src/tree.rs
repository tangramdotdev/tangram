use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;

/// Display a tree for a build or value.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The maximum depth to render.
	#[arg(long)]
	pub depth: Option<u32>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long, default_value = "false")]
	pub locked: bool,

	/// The reference to display a tree for.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_tree(&self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await.map_err(
			|source| tg::error!(!source, %reference = args.reference, "failed to get the reference"),
		)?;
		let options = crate::view::tree::Options {
			depth: args.depth,
			objects: true,
			builds: true,
			collapse_builds_on_success: true,
		};
		Self::tree_inner(self.handle().await?.clone(), referent.item, options).await?;
		Ok(())
	}

	pub async fn tree_inner(
		handle: impl tg::Handle,
		item: Either<tg::Build, tg::Object>,
		options: crate::view::tree::Options,
	) -> tg::Result<()> {
		let mut stdout = std::io::stdout();

		// Create the tree.
		let tree = crate::view::tree::Tree::new(&handle, item.clone(), options);

		// Render the tree until it is finished.
		loop {
			// Clear.
			let action = crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown);
			crossterm::execute!(stdout, action).unwrap();

			// If the tree is finished, then break.
			if tree.is_finished() {
				break;
			}

			// Save the current position.
			let action = crossterm::cursor::SavePosition;
			crossterm::execute!(stdout, action).unwrap();

			// Print the tree.
			println!("{}", tree.display());

			// Restore the cursor position.
			let action = crossterm::cursor::RestorePosition;
			crossterm::execute!(stdout, action).unwrap();

			// Sleep
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}

		// Print the tree.
		println!("{}", tree.display());

		Ok(())
	}
}
