use crate::{tui, Cli};
use tangram_client as tg;

/// View a build, object, package, or value.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: Option<Build>,

	#[command(flatten)]
	pub object: Option<Object>,

	#[command(flatten)]
	pub package: Option<Package>,

	#[arg(group = "default")]
	pub arg: Option<Arg>,
}

#[derive(Debug, clap::Args, Clone)]
pub struct Object {
	#[arg(short, long, group = "object")]
	pub object: bool,
	#[arg(short, long, group = "object")]
	pub object_: tg::object::Id,
}

#[derive(Debug, clap::Args, Clone)]
pub struct Package {
	#[arg(short, long, group = "package")]
	pub package: bool,
	#[arg(group = "package")]
	pub package_: tg::Dependency,
}

#[derive(Debug, clap::Args, Clone)]
pub struct Build {
	#[arg(short, long, group = "build")]
	pub build: bool,
	#[arg(group = "build")]
	pub build_: tg::build::Id,
}

#[derive(Clone, Debug)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
	Package(tg::Dependency),
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(dependency) = s.parse() {
			return Ok(Arg::Package(dependency));
		}
		if let Ok(build) = s.parse() {
			return Ok(Arg::Build(build));
		}
		if let Ok(object) = s.parse() {
			return Ok(Arg::Object(object));
		}
		Err(tg::error!(%s, "expected a build or object"))
	}
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let arg = if let Some(arg) = args.build {
			Arg::Build(arg.build_)
		} else if let Some(arg) = args.object {
			Arg::Object(arg.object_)
		} else if let Some(arg) = args.package {
			Arg::Package(arg.package_)
		} else if let Some(arg) = args.arg {
			arg
		} else {
			return Err(tg::error!("expected an object to view."));
		};

		let object = match arg {
			Arg::Build(build) => tui::Kind::Build(tg::Build::with_id(build)),
			Arg::Object(object) => tui::Kind::Value {
				value: tg::Object::with_id(object).into(),
				name: None,
			},
			Arg::Package(dependency) => {
				let (artifact, lock) = tg::package::get_with_lock(&self.handle, &dependency)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the package and lock"))?;
				tui::Kind::Package {
					dependency,
					artifact: Some(artifact),
					lock,
				}
			},
		};
		let tui = tui::Tui::start(&self.handle, object).await?;
		tui.wait().await?;
		Ok(())
	}
}
