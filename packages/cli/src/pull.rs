use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;

/// Pull processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub commands: bool,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,
}

impl Cli {
	pub async fn command_pull(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items = futures::future::try_join_all(referents.into_iter().map(async |referent| {
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

		// Pull the item.
		let arg = tg::pull::Arg {
			items: items.clone(),
			logs: args.logs,
			outputs: true,
			recursive: args.recursive,
			remote: remote.clone(),
			commands: args.commands,
		};
		let stream = handle.pull(arg).await?;
		self.render_progress_stream(stream).await?;

		// If any reference has a tag, then put it.
		futures::future::try_join_all(args.references.iter().enumerate().map(
			async |(idx, reference)| {
				if let tg::reference::Item::Tag(pattern) = reference.item() {
					if let Ok(tag) = pattern.clone().try_into() {
						let arg = tg::tag::put::Arg {
							force: false,
							item: items[idx].clone(),
							remote: Some(remote.clone()),
						};
						handle.put_tag(&tag, arg).await?;
					}
				}
				Ok::<_, tg::Error>(())
			},
		))
		.await?;

		Ok(())
	}
}
