use crate::{tree::Tree, Cli};
use async_recursion::async_recursion;
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
	Publish(PublishArgs),
	Search(SearchArgs),
	Tree(TreeArgs),
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

impl Cli {
	pub async fn command_package(&self, args: Args) -> Result<()> {
		match args.command {
			Command::Publish(args) => self.command_package_publish(args).await,
			Command::Search(args) => self.command_package_search(args).await,
			Command::Tree(args) => self.command_package_tree(args).await,
		}
	}

	pub async fn command_package_publish(&self, args: PublishArgs) -> Result<()> {
		let client = &self.client().await?;

		// Create the package.
		let (package, _) = tg::package::get_with_lock(client, &args.package).await?;

		// Get the package ID.
		let id = package.id(client).await?;

		// Publish the package.
		client
			.publish_package(self.user.as_ref(), id)
			.await
			.map_err(|error| error!(source = error, "failed to publish the package"))?;

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

		let dependency = args.package;
		let (package, lock) = tg::package::get_with_lock(client, &dependency)
			.await
			.map_err(|error| error!(source = error, %dependency, "failed to get the lock"))?;

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
