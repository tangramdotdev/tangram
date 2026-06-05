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
	#[arg(long, overrides_with = "no_timeout", value_parser = humantime::parse_duration)]
	pub timeout: Option<Duration>,

	#[arg(long, overrides_with = "timeout")]
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
		match referent.item {
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process => {
				let args = crate::process::children::Args {
					length: None,
					locations,
					position: None,
					print,
					process: id.try_into()?,
					size: None,
					timeout: crate::process::children::Timeout {
						timeout: timeout.get(),
						no_timeout: timeout.no_timeout,
					},
				};
				self.command_process_children(args).await?;
			},
			item => {
				let edge = item.to_graph_edge()?;
				let object = edge
					.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?
					.id();
				let args = crate::object::children::Args {
					locations: locations.clone(),
					object,
					print,
				};
				self.command_object_children(args).await?;
			},
		}
		Ok(())
	}
}
