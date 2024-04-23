use crate::Cli;
use either::Either;
use tangram_client as tg;
use tg::Handle as _;

/// Add a root.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub name: String,
	pub id: Arg,
}

#[derive(Debug, Clone)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
}

impl Cli {
	pub async fn command_root_add(&self, args: Args) -> tg::Result<()> {
		let name = args.name;
		let id = match args.id {
			Arg::Build(id) => Either::Left(id),
			Arg::Object(id) => Either::Right(id),
		};
		let arg = tg::root::AddArg { name, id };
		self.handle.add_root(arg).await?;
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
		Err(tg::error!(%s, "expected a build or object"))
	}
}
