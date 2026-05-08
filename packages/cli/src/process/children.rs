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
	pub process: tg::process::Id,

	#[arg(long)]
	pub size: Option<u64>,

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
	pub async fn command_process_children(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let locations = args.locations.get();
		let process = tg::Process::<tg::Value>::new(
			args.process.clone(),
			locations.clone(),
			None,
			None,
			None,
			None,
		);
		let arg = tg::process::children::get::Arg {
			length: args.length,
			location: locations,
			position: args.position.map(std::io::SeekFrom::Start),
			size: args.size,
			timeout: args.timeout.get(),
		};
		let stream = process
			.children_with_handle(&client, arg)
			.await
			.map_err(
				|source| tg::error!(!source, id = %args.process, "failed to get the process children"),
			)?
			.map_ok(|child| child.to_data());
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
