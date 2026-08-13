use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// The object or process.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub timeout: Timeout,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Timeout {
	#[arg(id = "children.timeout.timeout", long = "timeout", overrides_with = "children.timeout.no_timeout", value_parser = humantime::parse_duration)]
	pub timeout: Option<Duration>,

	#[arg(
		id = "children.timeout.no_timeout",
		long = "no-timeout",
		overrides_with = "children.timeout.timeout"
	)]
	pub no_timeout: bool,
}

impl Timeout {
	fn get(&self) -> Option<Duration> {
		if self.no_timeout {
			None
		} else {
			self.timeout.or(Some(Duration::ZERO))
		}
	}
}

impl Cli {
	pub async fn command_children(&mut self, args: Args) -> tg::Result<()> {
		let reference_location = args.reference.options().location.clone();
		let mut options = args.reference.options().clone();
		if let Some(location) = args.locations.get() {
			options.location = Some(location);
		}
		let reference =
			tg::Reference::with_node_and_options(args.reference.node().clone(), options);
		let print = args.print;
		let timeout = args.timeout;

		let referent = self.resolve(&reference).await?;
		let locations = args
			.locations
			.with_fallback_location(referent.options.location.as_ref())
			.with_fallback_location_arg(reference_location);
		let is_process = matches!(
			referent.node(),
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map::<tg::process::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let options = process.options.into();
			let process = tg::Reference::with_node_and_options(
				tg::reference::Node::Id(process.node.into()),
				options,
			);
			let args = crate::process::children::Args {
				length: None,
				locations,
				position: None,
				print,
				process,
				size: None,
				timeout: crate::process::children::Timeout {
					timeout: timeout.get(),
					no_timeout: timeout.no_timeout,
				},
			};
			self.command_process_children(args).await?;
		} else {
			let object = referent
				.into_graph_edge()?
				.try_map::<tg::object::Id, _>(|edge| {
					edge.try_unwrap_object()
						.map(|object| object.id())
						.map_err(|_| tg::error!("expected an object"))
				})?;
			let options = object.options.into();
			let object = tg::Reference::with_node_and_options(
				tg::reference::Node::Id(object.node.into()),
				options,
			);
			let args = crate::object::children::Args {
				locations,
				object,
				print,
			};
			self.command_object_children(args).await?;
		}
		Ok(())
	}
}
