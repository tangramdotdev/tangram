use crate::{util::TreeDisplay, Cli};
use async_recursion::async_recursion;
use console::style;
use either::Either;
use std::{collections::BTreeSet, fmt::Write};
use tangram_client as tg;
use tangram_error::{error, Result};

/// Manage pacakges.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
#[command(verbatim_doc_comment)]
pub enum Command {
	Publish(PublishArgs),
	Search(SearchArgs),
	Tree(TreeArgs),
}

#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct TreeArgs {
	#[arg(default_value = ".")]
	pub package: tg::Dependency,

	#[arg(short, long)]
	pub depth: Option<u32>,
}

/// Publish a package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PublishArgs {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

/// Search for packages.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct SearchArgs {
	pub query: String,
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
			.map_err(|error| error!(source = error, "Failed to publish the package."))?;

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
			.map_err(|error| error!(source = error, %dependency, "Failed to get lock."))?;

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
		println!("{tree}");
		Ok(())
	}
}

#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn get_package_tree(
	client: &dyn tg::Handle,
	dependency: &tg::Dependency,
	lock: &tg::Lock,
	node: usize,
	package: &tg::Directory,
	visited: &mut BTreeSet<tg::directory::Id>,
	current_depth: u32,
	max_depth: Option<u32>,
) -> Result<TreeDisplay> {
	// Create the data for the tree node, "<dependency>: <name>@version", or "<dependency>: <package_id>" if no metadata exists.
	let mut data = String::new();
	write!(data, "{}: ", style(dependency)).unwrap();
	if let Ok(metadata) = tg::package::get_metadata(client, package).await {
		if let Some(name) = metadata.name {
			write!(data, "{}", style(name)).unwrap();
		} else {
			write!(data, "{}", style(package)).unwrap();
		}
		if let Some(version) = metadata.version {
			write!(data, "@{}", style(version)).unwrap();
		}
	} else {
		write!(data, "{}", style(package)).unwrap();
	}

	// Skip the children of this node if we've already visited it or reached the max depth.
	let package_id = package.id(client).await?;
	if visited.contains(package_id)
		|| max_depth.map_or(false, |max_depth| current_depth == max_depth)
	{
		let children = Vec::new();
		return Ok(TreeDisplay { data, children });
	}
	visited.insert(package_id.clone());

	// Visit the children.
	let lock_data = lock.data(client).await?;
	let mut children = Vec::new();
	let node = lock_data
		.nodes
		.get(node)
		.ok_or_else(|| error!(%node, "Invalid lock. Node does not exist."))?;
	for (dependency, entry) in &node.dependencies {
		let Some(package) = entry.package.clone().map(tg::Directory::with_id) else {
			children.push(TreeDisplay {
				data: "<none>".into(),
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

	let tree = TreeDisplay { data, children };
	Ok(tree)
}
