use crate::Cli;
use futures::future;
use tangram_client::{self as tg, Handle};
use tangram_either::Either;

/// Push processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub commands: bool,

	#[arg(short, long)]
	pub force: bool,

	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,

	#[arg(short, long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_push(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items = future::try_join_all(referents.into_iter().map(async |referent| {
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
			Ok::<_, tg::Error>(item)
		}))
		.await?;

		// Push the item.
		let arg = tg::push::Arg {
			items: items.clone(),
			logs: args.logs,
			outputs: true,
			recursive: args.recursive,
			remote: remote.clone(),
			commands: args.commands,
		};
		let stream = handle.push(arg).await?;
		self.render_progress_stream(stream).await?;

		// If any reference has a tag, then put it.
		future::try_join_all(
			args.references
				.iter()
				.enumerate()
				.map(async |(idx, reference)| {
					if let tg::reference::Item::Tag(pattern) = reference.item() {
						if let Ok(tag) = pattern.clone().try_into() {
							let arg = tg::tag::put::Arg {
								force: args.force,
								item: items[idx].clone(),
								remote: Some(remote.clone()),
							};
							handle.put_tag(&tag, arg).await?;
						}
					}
					Ok::<_, tg::Error>(())
				}),
		)
		.await?;

		Ok(())
	}
}
