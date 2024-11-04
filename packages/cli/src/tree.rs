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
		Self::tree_inner(self.handle().await?.clone(), todo!(), options).await
	}

	pub async fn tree_inner(
		handle: impl tg::Handle,
		item: Either<tg::Build, tg::Object>,
		options: crate::view::tree::Options,
	) -> tg::Result<()> {
		let mut stdout = std::io::stdout();

		let tree = crate::view::tree::Tree::new(&handle, item.clone(), options);

		loop {
			// If the tree is done, then break.
			if tree.is_finished() {
				break;
			}

			// Save the current position.
			let action = crossterm::cursor::SavePosition;
			crossterm::execute!(stdout, action).unwrap();

			// Clear.
			let action = crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown);
			crossterm::execute!(stdout, action).unwrap();

			// Print the tree.
			tree.to_tree().print();

			// Restore the cursor position.
			let action = crossterm::cursor::RestorePosition;
			crossterm::execute!(stdout, action).unwrap();

			// Sleep
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}

		// Wait for the tree to be complete.
		tree.wait().await;

		// Print the tree.
		tree.to_tree().print();

		Ok(())
	}
}

#[derive(Debug)]
pub struct Tree {
	pub title: String,
	pub children: Vec<Self>,
}

impl Tree {
	pub fn print(&self) {
		self.print_inner("");
		println!();
	}

	fn print_inner(&self, prefix: &str) {
		print!("{}", self.title);
		for (n, child) in self.children.iter().enumerate() {
			print!("\n{prefix}");
			if n < self.children.len() - 1 {
				print!("├── ");
				child.print_inner(&format!("{prefix}│   "));
			} else {
				print!("└── ");
				child.print_inner(&format!("{prefix}    "));
			}
		}
	}
}
