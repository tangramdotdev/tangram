use crate::{tui, Cli};
use tangram_client as tg;

/// View a build, object, package, or value.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The build, package, or object to view.
	#[arg(conflicts_with_all = ["build", "object", "package"])]
	pub arg: Option<Arg>,

	/// The build to view.
	#[arg(long, conflicts_with_all = ["object", "package", "arg"])]
	pub build: Option<tg::build::Id>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The object to view.
	#[arg(long, conflicts_with_all = ["build", "package", "arg"])]
	pub object: Option<tg::object::Id>,

	/// The package to view.
	#[arg(long, conflicts_with_all = ["build", "object", "arg"])]
	pub package: Option<tg::Dependency>,
}

#[derive(Clone, Debug)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
	Package(tg::Dependency),
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		let arg = if let Some(arg) = args.build {
			Arg::Build(arg)
		} else if let Some(arg) = args.object {
			Arg::Object(arg)
		} else if let Some(arg) = args.package {
			Arg::Package(arg)
		} else if let Some(arg) = args.arg {
			arg
		} else {
			Arg::default()
		};

		// Get the item.
		let item = match arg {
			Arg::Build(build) => tui::Item::Build {
				build: tg::Build::with_id(build),
				remote: None,
			},
			Arg::Object(object) => tui::Item::Value {
				value: tg::Object::with_id(object).into(),
				name: None,
			},
			Arg::Package(mut dependency) => {
				// Canonicalize the path.
				if let Some(path) = dependency.path.as_mut() {
					*path = tokio::fs::canonicalize(&path)
						.await
						.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
						.try_into()?;
				}

				// Create the package and lock.
				let (package, lock) = tg::package::get_with_lock(&client, &dependency, args.locked)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the package and lock"))?;

				let path = dependency.path.clone();
				tui::Item::Package {
					dependency,
					artifact: Some(package),
					lock,
					path,
				}
			},
		};

		// Start the TUI.
		let tui = tui::Tui::start(&client, item).await?;

		// Wait for the TUI to finish.
		tui.wait().await?;

		Ok(())
	}
}

impl Default for Arg {
	fn default() -> Self {
		Arg::Package(tg::Dependency {
			path: Some(".".parse().unwrap()),
			..Default::default()
		})
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
		if let Ok(dependency) = s.parse() {
			return Ok(Arg::Package(dependency));
		}
		Err(tg::error!(%s, "expected a build or object"))
	}
}
