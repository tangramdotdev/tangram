use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;

/// Pull a process or an object.
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
	pub commands: bool,
}

impl Cli {
	pub async fn command_pull(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item {
			Either::Left(process) => Either::Left(process),
			Either::Right(object) => {
				let object = if let Some(subpath) = &referent.subpath {
					let directory = object
						.try_unwrap_directory()
						.ok()
						.ok_or_else(|| tg::error!("expected a directory"))?;
					directory.get(&handle, subpath).await?.into()
				} else {
					object
				};
				Either::Right(object)
			},
		};
		let item = match item {
			Either::Left(process) => Either::Left(process.id().clone()),
			Either::Right(object) => Either::Right(object.id(&handle).await?.clone()),
		};

		// Pull the item.
		match item.clone() {
			Either::Left(process) => {
				self.command_process_pull(crate::process::pull::Args {
					process,
					logs: args.logs,
					recursive: args.recursive,
					remote: args.remote,
					commands: args.commands,
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
		if let tg::reference::Item::Tag(pattern) = args.reference.item() {
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
