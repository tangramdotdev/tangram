use {
	crate::Cli,
	futures::{StreamExt as _, TryStreamExt as _},
	serde_with::serde_as,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_util::serde::SeekFromNumberOrString,
};

/// Get a process's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[arg(long)]
	pub length: Option<u64>,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(long, value_parser = parse_seek_from)]
	pub position: Option<std::io::SeekFrom>,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub size: Option<u64>,

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

#[serde_as]
#[derive(serde::Deserialize)]
struct PositionArg {
	#[serde_as(as = "SeekFromNumberOrString")]
	position: std::io::SeekFrom,
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
	pub async fn command_process_children(&mut self, args: Args) -> tg::Result<()> {
		let mut options = args.options;
		options
			.locations
			.set_from_reference_if_unset(&args.reference);
		let process = self
			.get_process_with_locations(&args.reference, &options.locations)
			.await?;
		self.command_process_children_inner(process, options).await
	}

	pub(crate) async fn command_process_children_inner(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = process.node.clone();
		let location = options.locations.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options_ = tg::process::children::get::Options {
			length: options.length,
			location,
			position: options.position,
			size: options.size,
			timeout: options.timeout.get(),
		};
		let stream = process
			.children_with_handle(&client, options_)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process children"))?
			.map_ok(|child| child.to_data());
		self.print_serde_stream(stream.boxed(), options.print)
			.await?;
		Ok(())
	}
}

fn parse_seek_from(value: &str) -> Result<std::io::SeekFrom, String> {
	let value = serde_json::json!({ "position": value });
	let position = serde_json::from_value::<PositionArg>(value)
		.map_err(|error| format!("failed to parse the position: {error}"))?;
	Ok(position.position)
}
