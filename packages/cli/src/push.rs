use crate::Cli;
use tangram_client::{self as tg, Handle};
use tangram_either::Either;

/// Push a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub force: bool,

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
	pub async fn command_push(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

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

		// Push the item.
		match item.clone() {
			Either::Left(process) => {
				let args = crate::process::push::Args {
					process,
					logs: args.logs,
					recursive: args.recursive,
					remote: Some(remote.clone()),
					commands: args.commands,
				};
				self.command_process_push(args).await?;
			},
			Either::Right(object) => {
				let args = crate::object::push::Args {
					object,
					remote: Some(remote.clone()),
				};
				self.command_object_push(args).await?;
			},
		}

		// If the reference has a tag, then put it.
		if let tg::reference::Item::Tag(pattern) = args.reference.item() {
			if let Ok(tag) = pattern.clone().try_into() {
				let arg = tg::tag::put::Arg {
					force: args.force,
					item,
					remote: Some(remote),
				};
				handle.put_tag(&tag, arg).await?;
			}
		}

		Ok(())
	}
}
