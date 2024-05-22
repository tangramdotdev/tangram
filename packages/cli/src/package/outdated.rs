use crate::Cli;
use crossterm::style::Stylize as _;
use itertools::Itertools as _;
use std::collections::VecDeque;
use tangram_client::{self as tg, Handle as _};

/// Get a package's outdated dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Print as JSON.
	#[arg(long)]
	pub json: bool,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The package.
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_package_outdated(&self, mut args: Args) -> tg::Result<()> {
		// Canonicalize the path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Get the outdated dependencies.
		let arg = tg::package::outdated::Arg {
			locked: args.locked,
		};
		let outdated = self
			.handle
			.get_package_outdated(&args.package, arg)
			.await
			.map_err(
				|source| tg::error!(!source, %package = args.package, "failed to get outdated packages"),
			)?;

		// Render the outdated dependencies as JSON if requested.
		if args.json {
			let json = serde_json::to_string_pretty(&outdated).map_err(
				|source| tg::error!(!source, %package = args.package, "failed to serialize outdated packages"),
			)?;
			println!("{json}");
			return Ok(());
		}

		// Flatten the tree.
		let mut queue = VecDeque::new();
		queue.push_back((vec![args.package.clone()], outdated));

		// Render the outdated dependencies.
		while let Some((path, outdated)) = queue.pop_front() {
			let tg::package::outdated::Output { info, dependencies } = outdated;
			if let Some(info) = info {
				let path = path.iter().map(ToString::to_string).join(" -> ");
				let tg::package::outdated::Info {
					current,
					compatible,
					latest,
					yanked,
				} = info;
				println!("{}", path.white().dim());
				print!(
					"{} = {}, {} = {}, {} = {}",
					"current".red(),
					current.green(),
					"compatible".red(),
					compatible.green(),
					"latest".red(),
					latest.green(),
				);
				if yanked {
					println!(" {}", "YANKED".red().bold());
				} else {
					println!();
				}
			}
			for (dependency, outdated) in dependencies {
				let mut path = path.clone();
				path.push(dependency);
				queue.push_back((path, outdated));
			}
		}

		Ok(())
	}
}
