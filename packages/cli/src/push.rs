use crate::Cli;
use tangram_client as tg;

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
	pub async fn command_push(&self, args: Args) -> tg::Result<()> {
		match args.arg {
			Arg::Build(id) => {
				self.command_build_push(super::build::push::Args { id })
					.await?;
			},
			Arg::Object(id) => {
				self.command_object_push(super::object::push::Args { id })
					.await?;
			},
		}
		Ok(())
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
		Err(tg::error!(%s, "expected a build or an object"))
	}
}
