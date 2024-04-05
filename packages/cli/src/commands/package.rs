use crate::{tree::Tree, Cli};
use crossterm::style::Stylize;
use itertools::Itertools;
use std::{
	collections::{BTreeSet, VecDeque},
	fmt::Write,
};
use tangram_client as tg;

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

/// List a package's outdated dependencies.
#[derive(Debug, clap::Args)]
pub struct OutdatedArgs {
	/// Print as JSON.
	#[clap(long)]
	pub json: bool,

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

/// Display a package's dependencies.
#[derive(Debug, clap::Args)]
pub struct TreeArgs {
	#[clap(default_value = ".")]
	pub package: tg::Dependency,

	#[clap(short, long)]
	pub depth: Option<u32>,
}

/// Update a package's lockfile.
#[derive(Debug, clap::Args)]
pub struct UpdateArgs {
	#[clap(short, long, default_value = ".")]
	pub path: tg::Path,
}

impl Cli {
	pub async fn command_package(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Outdated(args) => self.command_package_outdated(args).await,
			Command::Publish(args) => self.command_package_publish(args).await,
			Command::Search(args) => self.command_package_search(args).await,
			Command::Tree(args) => self.command_package_tree(args).await,
			Command::Update(args) => self.command_package_update(args).await,
		}
	}

	pub async fn command_package_outdated(&self, args: OutdatedArgs) -> tg::Result<()> {
		let client = &self.client().await?;
		let mut dependency = tg::Dependency::with_path(args.path);

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		let outdated = client.get_package_outdated(&dependency).await.map_err(
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
			let tg::package::OutdatedOutput { info, dependencies } = outdated;
			if let Some(info) = info {
				let path = path.iter().map(ToString::to_string).join(" -> ");
				let tg::package::OutdatedInfo {
					current,
					compatible,
					latest,
				} = info;
				println!("{}", path.white().dim());
				println!(
					"{} = {}, {} = {}, {} = {}",
					"current".red(),
					current.green(),
					"compatible".red(),
					compatible.green(),
					"latest".red(),
					latest.green(),
				);
			}
			for (dependency, outdated) in dependencies {
				let mut path = path.clone();
				path.push(dependency);
				queue.push_back((path, outdated));
			}
		}

		Ok(())
	}

	pub async fn command_package_publish(&self, args: PublishArgs) -> tg::Result<()> {
		let client = &self.client().await?;
		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, _) = tg::package::get_with_lock(client, &dependency).await?;

		// Get the package ID.
		let id = package.id(client).await?;

		// Publish the package.
		client
			.publish_package(self.user.as_ref(), &id)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the package"))?;

		Ok(())
	}

	pub async fn command_package_search(&self, args: SearchArgs) -> tg::Result<()> {
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

	pub async fn command_package_tree(&self, args: TreeArgs) -> tg::Result<()> {
		let client = &self.client().await?;
		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(client, &dependency)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to get the lock"))?;

		let mut visited = BTreeSet::new();
		let tree = get_package_tree(
			client,
			dependency,
			package,
			lock,
			&mut visited,
			1,
			args.depth,
		)
		.await?;

		tree.print();

		Ok(())
	}

	pub async fn command_package_update(&self, args: UpdateArgs) -> tg::Result<()> {
		let client = &self.client().await?;
		let mut dependency = tg::Dependency::with_path(args.path);

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to canonicalize the path"))?
				.try_into()?;
			tokio::fs::remove_file(path.clone().join("tangram.lock"))
				.await
				.ok();
		}

		let _ = tg::package::get_with_lock(client, &dependency)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to create a new lock"))?;

		Ok(())
	}
}

#[allow(clippy::too_many_arguments)]
async fn get_package_tree(
	client: &tg::Client,
	dependency: tg::Dependency,
	package: tg::Directory,
	lock: tg::Lock,
	visited: &mut BTreeSet<tg::directory::Id>,
	current_depth: u32,
	max_depth: Option<u32>,
) -> tg::Result<Tree> {
	let mut title = String::new();
	write!(title, "{dependency}: ").unwrap();
	if let Ok(metadata) = tg::package::get_metadata(client, &package).await {
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
	if visited.contains(&package_id)
		|| max_depth.map_or(false, |max_depth| current_depth == max_depth)
	{
		let children = Vec::new();
		return Ok(Tree { title, children });
	}
	visited.insert(package_id);

	let mut children = Vec::new();
	for dependency in lock.dependencies(client).await? {
		let (child_package, lock) = lock.get(client, &dependency).await?;
		let package = match (child_package, &dependency.path) {
			(Some(package), _) => package,
			(None, Some(path)) => package
				.get(client, path)
				.await
				.map_err(|source| tg::error!(!source, %path, "could not resolve path dependency"))?
				.try_unwrap_directory()
				.map_err(|source| tg::error!(!source, "expected a directory"))?,
			(None, None) => return Err(tg::error!("invalid lock")),
		};
		let child = Box::pin(get_package_tree(
			client,
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
