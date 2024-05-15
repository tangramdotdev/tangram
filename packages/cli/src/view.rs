use crate::{tui::Tui, Cli};
use either::Either;
use tangram_client as tg;

/// View a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The build or object to view.
	pub object: BuildOrObject,
}

#[derive(Clone, Debug)]
pub enum BuildOrObject {
	Build(tg::build::Id),
	Object(tg::object::Id),
}

impl std::str::FromStr for BuildOrObject {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(build) = s.parse() {
			return Ok(BuildOrObject::Build(build));
		}
		if let Ok(object) = s.parse() {
			return Ok(BuildOrObject::Object(object));
		}
		Err(tg::error!(%s, "expected a build or object"))
	}
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let object = match args.object {
			BuildOrObject::Build(build) => Either::Left(tg::Build::with_id(build)),
			BuildOrObject::Object(object) => Either::Right(tg::Object::with_id(object).into()),
		};
		let tui = Tui::start(&self.handle, object).await?;
		tui.wait().await?;
		Ok(())
	}
}
