use crate::Cli;
use either::Either;
use tangram_client as tg;
use tg::Handle as _;

/// Put a root.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub name: String,
	pub arg: Arg,
}

#[derive(Clone, Debug)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
}

impl Cli {
	pub async fn command_root_put(&self, args: Args) -> tg::Result<()> {
		let name = args.name;
		let item = match args.arg {
			Arg::Build(build) => Either::Left(build),
			Arg::Object(object) => Either::Right(object),
		};
		let arg = tg::root::put::Arg { item };
		self.handle.put_root(&name, arg).await?;
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
