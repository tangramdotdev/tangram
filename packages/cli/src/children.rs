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
		let reference = args.locations.apply_to_reference(&args.reference);
		let print = args.print;
		let timeout = args.timeout;

		let referent = self.resolve(&reference).await?;
		match referent.node {
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				let options = crate::process::children::Options {
					length: None,
					locations: crate::location::Args::default(),
					position: None,
					print,
					size: None,
					timeout: crate::process::children::Timeout {
						timeout: timeout.get(),
						no_timeout: timeout.no_timeout,
					},
				};
				self.command_process_children_inner(process, options)
					.await?;
			},
			node => {
				let object = tg::Referent::new(node, referent.options)
					.into_graph_edge()?
					.try_map::<tg::object::Id, _>(|edge| {
						edge.try_unwrap_object()
							.map(|object| object.id())
							.map_err(|_| tg::error!("expected an object"))
					})?;
				let options = crate::object::children::Options {
					locations: crate::location::Args::default(),
					print,
				};
				self.command_object_children_inner(object, options).await?;
			},
		}
		Ok(())
	}
}
