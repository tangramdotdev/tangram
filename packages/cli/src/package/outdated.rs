use crate::Cli;
use crossterm::style::Stylize as _;
use itertools::Itertools as _;
use std::collections::VecDeque;
use tangram_client::{self as tg, Handle as _};

/// Get a package's outdated dependencies.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Print as JSON.
	#[arg(long)]
	pub json: bool,

	#[arg(short, long, default_value = ".")]
	pub path: tg::Path,
}

impl Cli {
	pub async fn command_package_outdated(&self, args: Args) -> tg::Result<()> {
		let mut dependency = tg::Dependency::with_path(args.path);

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		let outdated = self
			.handle
			.get_package_outdated(&dependency)
			.await
			.map_err(
				|source| tg::error!(!source, %dependency, "failed to get outdated packages"),
			)?;

		if args.json {
			let json = serde_json::to_string_pretty(&outdated).map_err(
				|source| tg::error!(!source, %dependency, "failed to serialize outdated packages"),
			)?;
			println!("{json}");
			return Ok(());
		}

		// Flatten the tree.
		let mut queue = VecDeque::new();
		queue.push_back((vec![dependency.clone()], outdated));

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
