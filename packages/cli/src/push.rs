use {crate::Cli, futures::future, tangram_client::prelude::*};

/// Push processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(alias = "command", long)]
	pub commands: bool,

	#[command(flatten)]
	pub destination: crate::location::Args,

	#[command(flatten)]
	pub eager: Eager,

	#[arg(alias = "error", long)]
	pub errors: bool,

	#[arg(long, short)]
	pub force: bool,

	#[arg(alias = "log", long)]
	pub logs: bool,

	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub outputs: Outputs,

	#[arg(long)]
	pub recursive: bool,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Eager {
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "lazy",
		require_equals = true,
	)]
	eager: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "eager",
		require_equals = true,
	)]
	lazy: Option<bool>,
}

impl Eager {
	pub fn get(&self) -> bool {
		self.eager.or(self.lazy.map(|v| !v)).unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Outputs {
	#[arg(
		alias = "output",
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_outputs",
		require_equals = true,
	)]
	outputs: Option<bool>,

	#[arg(
		alias = "no-output",
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "outputs",
		require_equals = true,
	)]
	no_outputs: Option<bool>,
}

impl Outputs {
	pub fn get(&self) -> bool {
		self.outputs.or(self.no_outputs.map(|v| !v)).unwrap_or(true)
	}
}

impl Cli {
	pub async fn command_push(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let destination = args.destination.to_location()?;
		let location = destination.clone().or_else(|| {
			Some(tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			}))
		});

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items = referents
			.into_iter()
			.map(|referent| {
				referent
					.item
					.map_left(|object| object.id().clone())
					.map_right(|process| process.id().unwrap_right().clone())
			})
			.collect::<Vec<_>>();

		// Push the items.
		let arg = tg::push::Arg {
			commands: args.commands,
			destination: destination.clone(),
			eager: args.eager.get(),
			errors: args.errors,
			force: args.force,
			items: items.clone(),
			logs: args.logs,
			metadata: args.metadata,
			outputs: args.outputs.get(),
			recursive: args.recursive,
			source: None,
		};
		let stream = handle
			.push(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to push"))?;
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
