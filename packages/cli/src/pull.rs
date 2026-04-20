use {crate::Cli, futures::future, tangram_client::prelude::*};

/// Pull processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(alias = "command", long)]
	pub commands: bool,

	#[command(flatten)]
	pub eager: crate::push::Eager,

	#[arg(alias = "error", long)]
	pub errors: bool,

	#[arg(long, short)]
	pub force: bool,

	#[arg(alias = "log", long)]
	pub logs: bool,

	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub outputs: crate::push::Outputs,

	#[arg(long)]
	pub recursive: bool,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,

	#[command(flatten)]
	pub source: crate::location::Args,
}

impl Cli {
	pub async fn command_pull(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let source = args.source.to_location()?;
		let location = Some(tg::Location::Local(tg::location::Local::default()));

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
			destination: None,
			eager: args.eager.get(),
			errors: args.errors,
			force: args.force,
			items: items.clone(),
			logs: args.logs,
			metadata: args.metadata,
			outputs: args.outputs.get(),
			recursive: args.recursive,
			source,
		};
		let stream = handle
			.pull(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to pull"))?;
		let output = self.render_progress_stream(stream).await?;

		let processes = output.skipped.processes;
		let objects = output.skipped.objects;
		let bytes = byte_unit::Byte::from_u64(output.skipped.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!("skipped {processes} processes, {objects} objects, {bytes:#.1}");
		Self::print_info_message(&message);
		let processes = output.transferred.processes;
		let objects = output.transferred.objects;
		let bytes = byte_unit::Byte::from_u64(output.transferred.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!("transferred {processes} processes, {objects} objects, {bytes:#.1}");
		Self::print_info_message(&message);

		// Put tags.
		future::try_join_all(std::iter::zip(&args.references, &items).map(
			async |(reference, item)| {
				if let tg::reference::Item::Tag(pattern) = reference.item()
					&& let Ok(tag) = pattern.clone().try_into()
				{
					let arg = tg::tag::put::Arg {
						force: args.force,
						item: item.clone(),
						location: location.clone().map(Into::into),
						replicate: false,
					};
					handle
						.put_tag(&tag, arg)
						.await
						.map_err(|source| tg::error!(!source, %tag, "failed to put the tag"))?;
				}
				Ok::<_, tg::Error>(())
			},
		))
		.await?;
		for (reference, item) in std::iter::zip(&args.references, &items) {
			if reference.item().is_tag() {
				let message = format!("tagged {reference} {item}");
				Self::print_info_message(&message);
			}
		}

		Ok(())
	}
}
