use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Error, Result};

/// Get a build or an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	pub arg: Arg,
}

#[derive(Debug, Clone)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
}

impl Cli {
	pub async fn command_get(&self, args: Args) -> Result<()> {
		match args.arg {
			Arg::Build(id) => {
				self.command_build_get(super::build::GetArgs { id }).await?;
			},
			Arg::Object(id) => {
				self.command_object_get(super::object::GetArgs { id })
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
		Err(error!(%s, "expected a build id or an object id"))
	}
}
