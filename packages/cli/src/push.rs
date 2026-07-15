use {crate::Cli, futures::future, itertools::Itertools as _, tangram_client::prelude::*};

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
		id = "push.eager.eager",
		long = "eager",
		num_args = 0..=1,
		overrides_with = "push.eager.lazy",
		require_equals = true,
	)]
	eager: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "push.eager.lazy",
		long = "lazy",
		num_args = 0..=1,
		overrides_with = "push.eager.eager",
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
		id = "push.outputs.outputs",
		long = "outputs",
		num_args = 0..=1,
		overrides_with = "push.outputs.no_outputs",
		require_equals = true,
	)]
	outputs: Option<bool>,

	#[arg(
		alias = "no-output",
		default_missing_value = "true",
		id = "push.outputs.no_outputs",
		long = "no-outputs",
		num_args = 0..=1,
		overrides_with = "push.outputs.outputs",
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
		let client = self.client().await?;
		let destination = args.destination.to_location()?;
		let location = destination.clone().or_else(|| {
			Some(tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			}))
		});

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items: Vec<_> = referents
			.into_iter()
			.map(|referent| {
				let item = match referent.item {
					tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process => {
						tg::Either::Right(id.try_into()?)
					},
					tg::get::Item::Id(id) => tg::Either::Left(id.try_into()?),
					tg::get::Item::Pointer(_) => {
						return Err(tg::error!("expected an object or process id"));
					},
				};
				let item = if let Some(token) = referent.options.token {
					tg::Either::Right(tg::WithToken { id: item, token })
				} else {
					tg::Either::Left(item)
				};
				Ok::<_, tg::Error>(item)
			})
			.try_collect()?;

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
		let stream = client
			.push(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to push"))?;
		let output = self.render_progress_stream(stream).await?;

		let processes = output.skipped.processes;
		let objects = output.skipped.objects;
		let bytes = byte_unit::Byte::from_u64(output.skipped.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!("skipped {processes} processes, {objects} objects, {bytes:#.1}");
		self.print_info_message(&message);
		let processes = output.transferred.processes;
		let objects = output.transferred.objects;
		let bytes = byte_unit::Byte::from_u64(output.transferred.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!("transferred {processes} processes, {objects} objects, {bytes:#.1}");
		self.print_info_message(&message);

		// Put tags.
		future::try_join_all(std::iter::zip(&args.references, &items).map(
			async |(reference, item)| {
				if let Ok(specifier) = reference.item().try_unwrap_specifier_ref() {
					let arg = tg::tag::put::Arg {
						force: args.force,
						item: tag_item_from_item(item),
						location: location.clone().map(Into::into),
						public: false,
						specifier: specifier.clone().try_into()?,
					};
					client.put_tag(arg).await.map_err(
						|error| tg::error!(!error, tag = %specifier, "failed to put the tag"),
					)?;
				}
				Ok::<_, tg::Error>(())
			},
		))
		.await?;
		for (reference, item) in std::iter::zip(&args.references, &items) {
			if reference.item().is_specifier() {
				let item = item_id(item);
				let message = format!("tagged {} {item}", reference.without_token());
				self.print_info_message(&message);
			}
		}

		Ok(())
	}
}

fn item_id(
	item: &tg::MaybeWithToken<tg::Either<tg::object::Id, tg::process::Id>>,
) -> &tg::Either<tg::object::Id, tg::process::Id> {
	match item {
		tg::Either::Left(item) => item,
		tg::Either::Right(item) => &item.id,
	}
}

fn tag_item_from_item(
	item: &tg::MaybeWithToken<tg::Either<tg::object::Id, tg::process::Id>>,
) -> tg::tag::data::Item {
	item_id(item).clone().into()
}
