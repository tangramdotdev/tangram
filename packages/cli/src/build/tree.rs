use crate::{tree::Tree, Cli};
use crossterm::style::Stylize as _;
use futures::{stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use std::fmt::Write as _;
use tangram_client as tg;

/// Display the build tree.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,

	#[arg(long)]
	pub depth: Option<u32>,
}

impl Cli {
	pub async fn command_build_tree(&self, args: Args) -> tg::Result<()> {
		let build = tg::Build::with_id(args.build);
		let tree = self.get_build_tree(&build, 0, args.depth).await?;
		tree.print();
		Ok(())
	}

	async fn get_build_tree(
		&self,
		build: &tg::Build,
		current_depth: u32,
		max_depth: Option<u32>,
	) -> tg::Result<Tree> {
		// Get the build's metadata.
		let id = build.id().clone();
		let status = build
			.status(&self.handle)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the build's status"))?
			.next()
			.await
			.unwrap()
			.map_err(|source| tg::error!(!source, %id, "failed to get the build's status"))?;
		let target = build
			.target(&self.handle)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the build's target"))?;
		let package = target
			.package(&self.handle)
			.await
			.map_err(|source| tg::error!(!source, %target, "failed to get the target's package"))?;

		// Create the title.
		let mut title = String::new();
		match status {
			tg::build::Status::Created | tg::build::Status::Dequeued => {
				write!(title, "{}", "⟳ ".yellow()).unwrap();
			},
			tg::build::Status::Started => {
				write!(title, "{}", "⠿ ".blue()).unwrap();
			},
			tg::build::Status::Finished => {
				let outcome = build.outcome(&self.handle).await.map_err(
					|source| tg::error!(!source, %id, "failed to get the build outcome"),
				)?;
				match outcome {
					tg::build::Outcome::Canceled => {
						write!(title, "{}", "⦻ ".yellow()).unwrap();
					},
					tg::build::Outcome::Succeeded(_) => {
						write!(title, "{}", "✓ ".green()).unwrap();
					},
					tg::build::Outcome::Failed(_) => {
						write!(title, "{}", "✗ ".red()).unwrap();
					},
				}
			},
		}
		write!(title, "{} ", id.to_string().blue()).unwrap();
		if let Some(package) = package {
			if let Ok(metadata) =
				tg::package::get_metadata(&self.handle, &package.clone().into()).await
			{
				if let Some(name) = metadata.name {
					write!(title, "{}", name.magenta()).unwrap();
				} else {
					write!(title, "{}", "<unknown>".blue()).unwrap();
				}
				if let Some(version) = metadata.version {
					write!(title, "@{}", version.yellow()).unwrap();
				}
			} else {
				write!(title, "{}", "<unknown>".magenta()).unwrap();
			}
			write!(title, ":").unwrap();
		}

		// Get the build's children.
		let children = max_depth.map_or(true, |max_depth| current_depth < max_depth);
		let children = if children {
			let arg = tg::build::children::Arg::default();
			build
				.children(&self.handle, arg)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the build's children"))?
				.map(|child| async move {
					self.get_build_tree(&child?, current_depth + 1, max_depth)
						.await
				})
				.collect::<FuturesUnordered<_>>()
				.await
				.try_collect::<Vec<_>>()
				.await?
		} else {
			Vec::new()
		};

		// Create the tree.
		let tree = Tree { title, children };

		Ok(tree)
	}
}
