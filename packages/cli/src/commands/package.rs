use crate::{tree::Tree, Cli};
use async_recursion::async_recursion;
use dialoguer::console::style;
use either::Either;
use std::{collections::BTreeSet, fmt::Write};
use tangram_client as tg;
use tangram_error::{error, Result};

/// Manage packages.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Outdated(OutdatedArgs),
	Publish(PublishArgs),
	Search(SearchArgs),
	Tree(TreeArgs),
	Update(UpdateArgs),
}

/// List the available updates to a package.
#[derive(Debug, clap::Args)]
pub struct OutdatedArgs {
	#[clap(short, long, default_value = ".")]
	pub path: tg::Path,
}

/// Publish a package.
#[derive(Debug, clap::Args)]
pub struct PublishArgs {
	#[clap(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

/// Search for packages.
#[derive(Debug, clap::Args)]
pub struct SearchArgs {
	pub query: String,
}

/// Display the package tree.
#[derive(Debug, clap::Args)]
pub struct TreeArgs {
	#[clap(default_value = ".")]
	pub package: tg::Dependency,

	#[clap(short, long)]
	pub depth: Option<u32>,
}

/// Update an existing package's lock.
#[derive(Debug, clap::Args)]
pub struct UpdateArgs {
	#[clap(short, long, default_value = ".")]
	pub path: tg::Path,
}

impl Cli {
	pub async fn command_package(&self, args: Args) -> Result<()> {
		match args.command {
			Command::Outdated(args) => self.command_package_outdated(args).await,
			Command::Publish(args) => self.command_package_publish(args).await,
			Command::Search(args) => self.command_package_search(args).await,
			Command::Tree(args) => self.command_package_tree(args).await,
			Command::Update(args) => self.command_package_update(args).await,
		}
	}

	pub async fn command_package_outdated(&self, args: OutdatedArgs) -> Result<()> {
		let client = &self.client().await?;
		let mut dependency = tg::Dependency::with_path(args.path);

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Read the lock.
		let (package, lock) = tg::package::get_with_lock(client, &dependency)
			.await
			.map_err(|source| error!(!source, "failed to get lock"))?;

		// Write the lock.
		lock.write(client, dependency.path.as_ref().unwrap(), false)
			.await
			.map_err(|source| error!(!source, "failed to write the lock file"))?;

		let path = tg::package::try_get_metadata(client, &package)
			.await
			.map_err(|source| error!(!source, "failed to get root package metadata"))?
			.and_then(|metadata| metadata.name)
			.unwrap_or_else(|| "<unknown>".into());

		// Collect all the outdated packages.
		let mut visited = BTreeSet::new();
		let outdated = get_outdated(client, format!("{path:?}"), lock, &mut visited).await?;

		// Print the list of outdated packages.
		for outdated in outdated {
			let Outdated {
				compat,
				current,
				dependency,
				latest,
				path,
			} = outdated;

			println!("[{}.\"{}\"]", style(path).dim(), style(dependency).yellow());
			if let Some(current) = current {
				println!(
					"{} = {}",
					style("current").red(),
					style(format!("{current:?}")).green()
				);
			}
			if let Some(compat) = compat {
				println!(
					"{} = {}",
					style("compat").red(),
					style(format!("{compat:?}")).green()
				);
			}
			if let Some(latest) = latest {
				println!(
					"{} = {}",
					style("latest").red(),
					style(format!("{latest:?}")).green()
				);
			}
			println!();
		}
		Ok(())
	}

	pub async fn command_package_publish(&self, args: PublishArgs) -> Result<()> {
		let client = &self.client().await?;
		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(client, &dependency).await?;

		// Write the lock.
		if dependency.path.is_some() {
			let path = dependency.path.as_ref().unwrap();
			lock.write(client, path, false)
				.await
				.map_err(|source| error!(!source, "failed to write the lock file"))?;
		}

		// Get the package ID.
		let id = package.id(client).await?;

		// Publish the package.
		client
			.publish_package(self.user.as_ref(), id)
			.await
			.map_err(|source| error!(!source, "failed to publish the package"))?;

		Ok(())
	}

	pub async fn command_package_search(&self, args: SearchArgs) -> Result<()> {
		let client = &self.client().await?;

		// Perform the search.
		let arg = tg::package::SearchArg { query: args.query };
		let packages = client.search_packages(arg).await?;

		// Print the package names.
		if packages.is_empty() {
			println!("No packages matched your query.");
		}
		for package in packages {
			println!("{package}");
		}

		Ok(())
	}

	pub async fn command_package_tree(&self, args: TreeArgs) -> Result<()> {
		let client = &self.client().await?;
		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(client, &dependency)
			.await
			.map_err(|source| error!(!source, %dependency, "failed to get the lock"))?;

		// Write the lock.
		if dependency.path.is_some() {
			let path = dependency.path.as_ref().unwrap();
			lock.write(client, path, false)
				.await
				.map_err(|source| error!(!source, "failed to write the lock file"))?;
		}

		let mut visited = BTreeSet::new();
		let tree = get_package_tree(
			client,
			&dependency,
			&lock,
			0,
			&package,
			&mut visited,
			1,
			args.depth,
		)
		.await?;

		tree.print();

		Ok(())
	}

	pub async fn command_package_update(&self, args: UpdateArgs) -> Result<()> {
		let client = &self.client().await?;
		let mut dependency = tg::Dependency::with_path(args.path);

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		let lock = tg::package::create_lock(client, &dependency)
			.await
			.map_err(|source| error!(!source, %dependency, "failed to create a new lock"))?;

		// Overwrite the existing lock.
		lock.write(client, dependency.path.unwrap(), true)
			.await
			.map_err(|source| error!(!source, "failed to write the lock"))?;

		Ok(())
	}
}

#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn get_package_tree(
	client: &tg::Client,
	dependency: &tg::Dependency,
	lock: &tg::Lock,
	node: usize,
	package: &tg::Directory,
	visited: &mut BTreeSet<tg::directory::Id>,
	current_depth: u32,
	max_depth: Option<u32>,
) -> Result<Tree> {
	let mut title = String::new();
	write!(title, "{dependency}: ").unwrap();
	if let Ok(metadata) = tg::package::get_metadata(client, package).await {
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

	let package_id = package.id(client).await?;
	if visited.contains(package_id)
		|| max_depth.map_or(false, |max_depth| current_depth == max_depth)
	{
		let children = Vec::new();
		return Ok(Tree { title, children });
	}
	visited.insert(package_id.clone());

	let lock_data = lock.data(client).await?;
	let mut children = Vec::new();
	let node = lock_data
		.nodes
		.get(node)
		.ok_or_else(|| error!(%node, "invalid lock"))?;
	for (dependency, entry) in &node.dependencies {
		let Some(package) = entry.package.clone().map(tg::Directory::with_id) else {
			children.push(Tree {
				title: "<none>".into(),
				children: Vec::new(),
			});
			continue;
		};
		match &entry.lock {
			Either::Left(node) => {
				let child = get_package_tree(
					client,
					dependency,
					lock,
					*node,
					&package,
					visited,
					current_depth + 1,
					max_depth,
				)
				.await?;
				children.push(child);
			},
			Either::Right(lock) => {
				let lock = tg::Lock::with_id(lock.clone());
				let mut visited = BTreeSet::new();
				let child = get_package_tree(
					client,
					dependency,
					&lock,
					0,
					&package,
					&mut visited,
					current_depth + 1,
					max_depth,
				)
				.await?;
				children.push(child);
			},
		}
	}

	let tree = Tree { title, children };

	Ok(tree)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Outdated {
	compat: Option<String>,
	current: Option<String>,
	dependency: tg::Dependency,
	latest: Option<String>,
	path: String,
}

#[async_recursion]
async fn get_outdated(
	tg: &dyn tg::Handle,
	path: String,
	lock: tg::Lock,
	visited: &mut BTreeSet<tg::lock::Id>,
) -> Result<Vec<Outdated>> {
	let id = lock.id(tg).await?;
	if visited.contains(id) {
		return Ok(Vec::new());
	}
	visited.insert(id.clone());

	let data = lock
		.data(tg)
		.await
		.map_err(|error| error!(source = error, "failed to get the lock data"))?;

	let dependencies = &data.nodes[data.root].dependencies;
	let mut outdated = Vec::with_capacity(dependencies.len());

	// Check each dependency.
	for dependency in dependencies.keys() {
		let (package, lock) = lock
			.get(tg, dependency)
			.await?
			.ok_or_else(|| error!(%dependency, "invalid lock"))?;

		// Get the metadata for this package.
		let metadata = tg::package::get_metadata(tg, &package).await.ok();
		let (name, current) = if let Some(metadata) = metadata {
			(metadata.name, metadata.version)
		} else {
			(None, None)
		};

		// Get all versions of this package.map
		let dependency_name = dependency.name.as_ref().unwrap().clone();
		let all_versions = tg
			.get_package_versions(&tg::Dependency::with_name(dependency_name))
			.await
			.map_err(|source| error!(!source, "failed to get package versions"))?;
		let compat = all_versions
			.iter()
			.rev()
			.find(|version| dependency.try_match_version(version).unwrap_or(false))
			.cloned();
		let latest = all_versions.last().cloned();

		if dependency.path.is_none() && latest != current {
			let dependency = dependency.clone();
			let path = path.clone();
			outdated.push(Outdated {
				compat,
				current,
				dependency,
				latest,
				path,
			});
		}

		// Visit the dependency's Lock to collect any transitively outdated dependencies.
		let path = format!("{path}.{:?}", name.as_deref().unwrap_or("<unknown>"));
		let transitive_outdated = get_outdated(tg, path.clone(), lock, visited).await?;
		outdated.extend(transitive_outdated);
	}
	Ok(outdated)
}
