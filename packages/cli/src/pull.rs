use crate::Cli;
use crossterm::style::Stylize as _;
use futures::future;
use tangram_client::{self as tg, prelude::*};

pub type Args = crate::push::Args;

impl Cli {
	pub async fn command_pull(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items = referents
			.into_iter()
			.map(|referent| {
				referent
					.item
					.map_left(|process| process.id().clone())
					.map_right(|object| object.id().clone())
			})
			.collect::<Vec<_>>();

		// Pull the items.
		let arg = tg::pull::Arg {
			commands: args.commands,
			items: items.clone(),
			logs: args.logs,
			outputs: true,
			recursive: args.recursive,
			remote: Some(remote.clone()),
		};
		let stream = handle.pull(arg).await?;
		let output = self.render_progress_stream(stream).await?;
		eprintln!(
			"{} pulled {} processes, {} objects, {} bytes",
			"info".blue().bold(),
			output.processes,
			output.objects,
			output.bytes,
		);

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
