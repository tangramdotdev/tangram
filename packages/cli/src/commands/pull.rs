use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Error, Result};

/// Pull a build or an object.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub arg: Arg,
}

#[derive(Debug, Clone)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
}

impl Cli {
	pub async fn command_pull(&self, args: Args) -> Result<()> {
		match args.arg {
			Arg::Build(id) => {
				self.command_build_pull(super::build::PullArgs { id })
					.await?;
			},
			Arg::Object(id) => {
				self.command_object_pull(super::object::PullArgs { id })
					.await?;
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
		Err(error!(%s, "expected a build or an object"))
	}
}
