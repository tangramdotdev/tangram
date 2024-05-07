use crate::{tree::Tree, Cli};
use std::{collections::BTreeSet, fmt::Write};
use tangram_client as tg;

/// Display a package's dependencies.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".")]
	pub package: tg::Dependency,

	#[arg(short, long)]
	pub depth: Option<u32>,
}

impl Cli {
	pub async fn command_package_tree(&self, args: Args) -> tg::Result<()> {
		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(&self.handle, &dependency)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to get the lock"))?;

		let mut visited = BTreeSet::new();
		let tree = self
			.get_package_tree(dependency, package, lock, &mut visited, 1, args.depth)
			.await?;

		tree.print();

		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn get_package_tree(
		&self,
		dependency: tg::Dependency,
		package: tg::Directory,
		lock: tg::Lock,
		visited: &mut BTreeSet<tg::directory::Id>,
		current_depth: u32,
		max_depth: Option<u32>,
	) -> tg::Result<Tree> {
		let mut title = String::new();
		write!(title, "{dependency}: ").unwrap();
		if let Ok(metadata) = tg::package::get_metadata(&self.handle, &package).await {
			if let Some(name) = metadata.name {
				write!(title, "{name}").unwrap();
			} else {
				write!(title, "{package}").unwrap();
			}
			if let Some(version) = metadata.version {
				write!(title, "@{version}").unwrap();
			}
		} else {
			write!(title, "{package}").unwrap();
		}

		let package_id = package.id(&self.handle, None).await?;
		if visited.contains(&package_id)
			|| max_depth.map_or(false, |max_depth| current_depth == max_depth)
		{
			let children = Vec::new();
			return Ok(Tree { title, children });
		}
		visited.insert(package_id);

		let mut children = Vec::new();
		for dependency in lock.dependencies(&self.handle).await? {
			let (child_package, lock) = lock.get(&self.handle, &dependency).await?;
			let package = match (child_package, &dependency.path) {
				(Some(package), _) => package,
				(None, Some(path)) => package
					.get(&self.handle, path)
					.await
					.map_err(
						|source| tg::error!(!source, %path, "could not resolve path dependency"),
					)?
					.try_unwrap_directory()
					.map_err(|source| tg::error!(!source, "expected a directory"))?,
				(None, None) => return Err(tg::error!("invalid lock")),
			};
			let child = Box::pin(self.get_package_tree(
				dependency,
				package,
				lock,
				visited,
				current_depth,
				max_depth,
			))
			.await?;
			children.push(child);
		}

		let tree = Tree { title, children };

		Ok(tree)
	}
}
