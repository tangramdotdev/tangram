use {
	crate::Cli,
	crossterm::style::Stylize as _,
	futures::future,
	tangram_client::{self as tg, prelude::*},
};

/// Push processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(alias = "command", long)]
	pub commands: bool,

	#[arg(long, short)]
	pub force: bool,

	#[arg(alias = "log", long)]
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
		let items = referents
			.into_iter()
			.map(|referent| {
				referent
					.item
					.map_left(|process| process.id().clone())
					.map_right(|object| object.id().clone())
			})
			.collect::<Vec<_>>();

		// Push the items.
		let arg = tg::push::Arg {
			commands: args.commands,
			items: items.clone(),
			logs: args.logs,
			outputs: true,
			recursive: args.recursive,
			remote: Some(remote.clone()),
		};
		let stream = handle.push(arg).await?;
		let output = self.render_progress_stream(stream).await?;
		eprintln!(
			"{} pushed {} processes, {} objects, {} bytes",
			"info".blue().bold(),
			output.processes,
			output.objects,
			output.bytes,
		);

		// Put tags.
		future::try_join_all(std::iter::zip(&args.references, &items).map(
			async |(reference, item)| {
				if let tg::reference::Item::Tag(pattern) = reference.item()
					&& let Ok(tag) = pattern.clone().try_into()
				{
					let arg = tg::tag::put::Arg {
						force: args.force,
						item: item.clone(),
						remote: Some(remote.clone()),
					};
					handle.put_tag(&tag, arg).await?;
				}
				Ok::<_, tg::Error>(())
			},
		))
		.await?;
		for (reference, item) in std::iter::zip(&args.references, &items) {
			if reference.item().is_tag() {
				eprintln!("{} tagged {} {}", "info".blue().bold(), reference, item);
			}
		}

		Ok(())
	}
}
