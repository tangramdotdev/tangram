use crate::Cli;
use tangram_client as tg;

/// Display a build, object, or package tree.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".")]
	arg: Arg,

	#[arg(short, long)]
	depth: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
	Package(tg::Dependency),
}

#[derive(Debug)]
pub struct Tree {
	pub title: String,
	pub children: Vec<Self>,
}

impl Cli {
	pub async fn command_tree(&self, args: Args) -> tg::Result<()> {
		match args.arg {
			Arg::Build(id) => {
				let args = super::build::tree::Args {
					build: id,
					depth: args.depth,
				};
				self.command_build_tree(args).await?;
			},
			Arg::Object(id) => {
				let args = super::object::tree::Args {
					object: id,
					depth: args.depth,
				};
				self.command_object_tree(args).await?;
			},
			Arg::Package(package) => {
				let args = super::package::tree::Args {
					package,
					depth: args.depth,
				};
				self.command_package_tree(args).await?;
			},
		}
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

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(build) = s.parse() {
			return Ok(Arg::Build(build));
		}
		if let Ok(object) = s.parse() {
			return Ok(Arg::Object(object));
		}
		if let Ok(package) = s.parse() {
			return Ok(Arg::Package(package));
		}
		Err(tg::error!(%s, "expected a build, object, or dependency"))
	}
}
