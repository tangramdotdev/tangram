use crate::{tree::Tree, Cli};
use crossterm::style::Stylize as _;
use futures::{stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use std::fmt::Write;
use tangram_client as tg;

/// Display the build tree.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::build::Id,
	#[clap(long)]
	pub depth: Option<u32>,
}

impl Cli {
	pub async fn command_build_tree(&self, args: Args) -> tg::Result<()> {
		let build = tg::Build::with_id(args.id);
		let tree = get_build_tree(&self.handle, &build, 1, args.depth).await?;
		tree.print();
		Ok(())
	}
}

async fn get_build_tree<H>(
	handle: &H,
	build: &tg::Build,
	current_depth: u32,
	max_depth: Option<u32>,
) -> tg::Result<Tree>
where
	H: tg::Handle,
{
	// Get the build's metadata.
	let id = build.id().clone();
	let status = build
		.status(handle, tg::build::status::GetArg::default())
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to get the build's status"))?
		.next()
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, %id, "failed to get the build's status"))?;
	let target = build
		.target(handle)
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to get build's target"))?;
	let package = target
		.package(handle)
		.await
		.map_err(|source| tg::error!(!source, %target, "failed to get target's package"))?;
	let name = target
		.name(handle)
		.await
		.map_err(|source| tg::error!(!source, %target, "failed to get target's name"))?
		.clone()
		.unwrap_or_else(|| "<unknown>".into());

	// Render the title
	let mut title = String::new();
	match status {
		tg::build::Status::Created | tg::build::Status::Queued => {
			write!(title, "{}", "⟳".yellow()).unwrap();
		},
		tg::build::Status::Started => write!(title, "{}", "⠿".blue()).unwrap(),
		tg::build::Status::Finished => {
			let outcome = build
				.outcome(handle)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the build outcome"))?;
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
		if let Ok(metadata) = tg::package::get_metadata(handle, &package).await {
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
	write!(title, "{}", name.white()).unwrap();

	// Get the build's children.
	let children = if max_depth.map_or(true, |max_depth| current_depth < max_depth) {
		let arg = tg::build::children::GetArg {
			position: Some(std::io::SeekFrom::Start(0)),
			timeout: Some(std::time::Duration::ZERO),
			..Default::default()
		};
		build
			.children(handle, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the build's children"))?
			.map(|child| async move {
				get_build_tree(handle, &child?, current_depth + 1, max_depth).await
			})
			.collect::<FuturesUnordered<_>>()
			.await
			.try_collect::<Vec<_>>()
			.await?
	} else {
		Vec::new()
	};

	let tree = Tree { title, children };

	Ok(tree)
}
