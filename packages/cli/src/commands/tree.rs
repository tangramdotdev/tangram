use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Error, Result};

use super::{build, object, package};

/// Display a build, object, or package tree.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
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

impl std::str::FromStr for Arg {
	type Err = Error;
	fn from_str(arg: &str) -> Result<Self, Self::Err> {
		if let Ok(build) = arg.parse() {
			return Ok(Arg::Build(build));
		}
		if let Ok(object) = arg.parse() {
			return Ok(Arg::Object(object));
		}
		if let Ok(package) = arg.parse() {
			return Ok(Arg::Package(package));
		}
		Err(error!(%arg, "Expected a dependency, build id, or object id."))
	}
}

impl Cli {
	pub async fn command_tree(&self, args: Args) -> Result<()> {
		match args.arg {
			Arg::Build(id) => {
				let args = build::TreeArgs {
					id,
					depth: args.depth,
				};
				self.command_build_tree(args).await?;
			},
			Arg::Object(id) => {
				let args = object::TreeArgs {
					id,
					depth: args.depth,
				};
				self.command_object_tree(args).await?;
			},
			Arg::Package(package) => {
				let args = package::TreeArgs {
					package,
					depth: args.depth,
				};
				self.command_package_tree(args).await?;
			},
		}
		Ok(())
	}
}
