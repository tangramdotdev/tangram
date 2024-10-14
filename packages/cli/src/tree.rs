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

#[derive(Debug)]
pub struct Tree {
	pub title: String,
	pub children: Vec<Self>,
}

impl Cli {
	pub async fn command_tree(&self, args: Args) -> tg::Result<()> {
		let item = self.get_reference(&args.reference).await.map_err(
			|source| tg::error!(!source, %reference = args.reference, "failed to get the reference"),
		)?;
		let expand_options = crate::view::tree::ExpandOptions {
			depth: args.depth,
			object_children: true,
			build_children: true,
			collapse_builds_on_success: true,
		};
		Self::print_tree(self.handle().await?.clone(), item, expand_options).await
	}

	pub async fn print_tree(
		handle: impl tg::Handle,
		item: Either<tg::Build, tg::Object>,
		expand_options: crate::view::tree::ExpandOptions,
	) -> tg::Result<()> {
		let tree = crate::view::tree::Tree::new(&handle, item.clone(), expand_options);

		if item.is_left() {
			loop {
				// Save the current position.
				crossterm::execute!(std::io::stdout(), crossterm::cursor::SavePosition).unwrap();

				// Clear.
				crossterm::execute!(
					std::io::stdout(),
					crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown),
				)
				.unwrap();

				// Print the tree.
				tree.to_tree().print();

				// Sleep
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;

				// Restore the cursor position.
				crossterm::execute!(std::io::stdout(), crossterm::cursor::RestorePosition).unwrap();

				// Check if we need to exit.
				if tree.is_done() {
					break;
				}
			}
		}

		// Wait for the tree to be complete.
		tree.wait().await;

		// Print the tree.
		tree.to_tree().print();

		Ok(())
	}
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
