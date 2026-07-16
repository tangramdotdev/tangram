use {
	crate::Cli,
	futures::{StreamExt as _, TryStreamExt as _},
	std::time::Duration,
	tangram_client::prelude::*,
};

/// Get a process's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub length: Option<u64>,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(long)]
	pub position: Option<u64>,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::Reference,

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
		let client = self.client().await?;
		let locations = args.locations.get();
		let process = self.get_resolved_process(&args.process).await?;
		let id = process.item;
		let token = process.options.token;
		let process = tg::Process::<tg::Value>::new(
			id.clone(),
			tg::process::Options {
				location: locations.clone(),
				token: token.clone(),
				..Default::default()
			},
		);
		let arg = tg::process::children::get::Arg {
			length: args.length,
			location: locations,
			position: args.position.map(std::io::SeekFrom::Start),
			size: args.size,
			timeout: args.timeout.get(),
			token: None,
		};
		let stream = process
			.children_with_handle(&client, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process children"))?
			.map_ok(|child| child.to_data());
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
