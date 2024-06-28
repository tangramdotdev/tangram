use crate::{tree::Tree, Cli};
use std::{collections::BTreeSet, fmt::Write};
use tangram_client as tg;

/// Display a package's dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub depth: Option<u32>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_package_tree(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(&client, &dependency, args.locked).await?;

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
		package: tg::Artifact,
		lock: tg::Lock,
		visited: &mut BTreeSet<tg::artifact::Id>,
		current_depth: u32,
		max_depth: Option<u32>,
	) -> tg::Result<Tree> {
		let client = self.client().await?;
		let mut title = String::new();
		write!(title, "{dependency}: ").unwrap();
		if let Ok(metadata) = tg::package::get_metadata(&client, &package).await {
			if let Some(name) = metadata.name {
				write!(title, "{name}").unwrap();
			} else {
				todo!()
			}
			if let Some(version) = metadata.version {
				write!(title, "@{version}").unwrap();
			}
		} else {
			todo!()
		}

		let package_id = package.id(&client).await?;
		if visited.contains(&package_id)
			|| max_depth.map_or(false, |max_depth| current_depth == max_depth)
		{
			let children = Vec::new();
			return Ok(Tree { title, children });
		}
		visited.insert(package_id);

		let mut children = Vec::new();
		for dependency in lock.dependencies(&client).await? {
			let (child_package, lock) = lock.get(&client, &dependency).await?;
			let package = match (child_package, &dependency.path) {
				(Some(package), _) => package,
				(None, Some(path)) => package
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?
					.get(&client, path)
					.await
					.map_err(
						|source| tg::error!(!source, %path, "could not resolve path dependency"),
					)?,
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
