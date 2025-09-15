use crate::Cli;
use futures::future;
use tangram_client::{self as tg, prelude::*};

/// Push processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub commands: bool,

	#[arg(long, short)]
	pub force: bool,

	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,

	#[arg(long, short)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_push(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items = future::try_join_all(referents.into_iter().map(async |referent| {
			let item = referent
				.item
				.map_left(|process| process.id().clone())
				.map_right(|object| object.id().clone());
			Ok::<_, tg::Error>(item)
		}))
		.await?;

		// Push the item.
		let arg = tg::push::Arg {
			items: items.clone(),
			logs: args.logs,
			outputs: true,
			recursive: args.recursive,
			remote: Some(remote.clone()),
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
