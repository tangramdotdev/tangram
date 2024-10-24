use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;

/// Pull a build or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub targets: bool,
}

impl Cli {
	pub async fn command_pull(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let item = self.get_reference(&args.reference).await?;

		// Get the item as an ID.
		let item = match item {
			Either::Left(build) => Either::Left(build.id().clone()),
			Either::Right(object) => Either::Right(object.id(&handle).await?.clone()),
		};

		// Pull the item.
		match item.clone() {
			Either::Left(build) => {
				self.command_build_pull(crate::build::pull::Args {
					build,
					logs: args.logs,
					recursive: args.recursive,
					remote: args.remote,
					targets: args.targets,
				})
				.await?;
			},
			Either::Right(object) => {
				self.command_object_pull(crate::object::pull::Args {
					object,
					remote: args.remote,
				})
				.await?;
			},
		}

		// If the reference has a tag, then put it.
		if let tg::reference::Item::Tag(pattern) = args.reference.path() {
			if let Ok(tag) = pattern.clone().try_into() {
				let arg = tg::tag::put::Arg {
					force: false,
					item,
					remote: None,
				};
				handle.put_tag(&tag, arg).await?;
			}
		}

		Ok(())
	}
}
