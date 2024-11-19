use std::pin::pin;

use crate::Cli;
use futures::future;
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

	/// The remote to use.
	#[arg(short, long)]
	pub remote: Option<String>,

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
		let mut done = false;
		loop {
			// Clear.
			let action = crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown);
			crossterm::execute!(stdout, action).unwrap();

			// If we're done waiting for the tree, then break.
			if done {
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

			// Sleep and wait for the tree to finish.
			let sleep = tokio::time::sleep(std::time::Duration::from_millis(100));
			if let future::Either::Left(_) = future::select(pin!(tree.wait()), pin!(sleep)).await {
				done = true;
			}
		}

		// Print the tree.
		println!("{}", tree.display());

		Ok(())
	}
}
