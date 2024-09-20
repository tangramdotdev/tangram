use crate::Cli;
use tangram_client as tg;

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
	pub async fn command_tree(&self, _args: Args) -> tg::Result<()> {
		Err(tg::error!("unimplemented"))
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
