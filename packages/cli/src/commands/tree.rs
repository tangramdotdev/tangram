use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Error, Result};

use super::{build, object, package};

/// Display a build, object, or package tree.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(default_value = ".")]
	arg: Arg,
	#[clap(short, long)]
	depth: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
	Package(tg::Dependency),
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

impl std::str::FromStr for Arg {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(build) = s.parse() {
			return Ok(Arg::Build(build));
		}
		if let Ok(object) = s.parse() {
			return Ok(Arg::Object(object));
		}
		if let Ok(package) = s.parse() {
			return Ok(Arg::Package(package));
		}
		Err(error!(%s, "expecte abuild id, object id, or dependency"))
	}
}
