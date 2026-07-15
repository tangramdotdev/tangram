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
		let locations = args.locations;
		let print = args.print;
		let timeout = args.timeout;

		let referent = self.get_resolved_reference(&args.reference).await?;
		let is_process = matches!(
			referent.item(),
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map::<tg::process::Id, _>(|item| match item {
				tg::get::Item::Id(id) => id.try_into(),
				tg::get::Item::Pointer(_) => unreachable!(),
			})?;
			let process = tg::Reference::with_item_and_token(
				tg::reference::Item::Id(process.item.into()),
				process.options.token,
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
			let object = tg::Reference::with_item_and_token(
				tg::reference::Item::Id(object.item.into()),
				object.options.token,
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
