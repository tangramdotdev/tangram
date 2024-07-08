use crate::Cli;
use either::Either;
use tangram_client::{self as tg, Handle as _};

/// Push a build or an object.
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
	pub async fn command_push(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the item.
		let item = args.reference.get(&handle).await?;
		let item = match item {
			Either::Left(build) => Either::Left(build.id().clone()),
			Either::Right(object) => Either::Right(object.id(&handle).await?.clone()),
		};

		// Push the item.
		match item.clone() {
			Either::Left(build) => {
				self.command_build_push(crate::build::push::Args {
					build,
					logs: args.logs,
					recursive: args.recursive,
					remote: args.remote,
					targets: args.targets,
				})
				.await?;
			},
			Either::Right(object) => {
				self.command_object_push(crate::object::push::Args {
					object,
					remote: args.remote,
				})
				.await?;
			},
		}

		// If the reference has a tag, then put it.
		if let tg::reference::Path::Tag(pattern) = args.reference.path() {
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
