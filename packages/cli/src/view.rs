use crate::{tui::Tui, Cli};
use either::Either;
use tangram_client as tg;

/// View a build, object, package, or value.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The item to view.
	pub arg: Arg,
}

#[derive(Clone, Debug)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
	Package(tg::Dependency),
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let object = match args.arg {
			Arg::Build(build) => Either::Left(tg::Build::with_id(build)),
			Arg::Object(object) => Either::Right(tg::Object::with_id(object).into()),
			Arg::Object(object) => Either::Right(tg::Object::with_id(object).into()),
		};
		let tui = Tui::start(&self.handle, object).await?;
		tui.wait().await?;
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
		if let Ok(package) = s.parse() {
			return Ok(Arg::Object(object));
		}
		Err(tg::error!(%s, "expected a build or object"))
	}
}
