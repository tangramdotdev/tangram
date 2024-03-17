use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Error, Result};

/// Push a build or an object.
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
	pub async fn command_push(&self, args: Args) -> Result<()> {
		match args.arg {
			Arg::Build(id) => {
				self.command_build_push(super::build::PushArgs { id })
					.await?;
			},
			Arg::Object(id) => {
				self.command_object_push(super::object::PushArgs { id })
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
