use crate::Cli;
use futures::future;
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;

/// Pull processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub commands: bool,

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
	pub async fn command_pull(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items = referents
			.into_iter()
			.map(|referent| match referent.item {
				Either::Left(process) => Either::Left(process.id().clone()),
				Either::Right(object) => Either::Right(object.id().clone()),
			})
			.collect::<Vec<_>>();

		// Pull the item.
		let arg = tg::pull::Arg {
			items: items.clone(),
			logs: args.logs,
			outputs: true,
			recursive: args.recursive,
			remote: Some(remote.clone()),
			commands: args.commands,
		};
		let stream = handle.pull(arg).await?;
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
								force: false,
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
