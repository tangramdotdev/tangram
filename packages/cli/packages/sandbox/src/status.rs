use {
	futures::{StreamExt as _, TryStreamExt as _, future},
	std::{
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
	tangram_futures::stream::Ext as _,
};

/// Get a sandbox's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub sandbox: tg::Referent<tg::sandbox::Id>,

	#[command(flatten)]
	pub timeout: Timeout,

	/// Wait until the sandbox's status is this status, and fail if the sandbox can no longer have it.
	#[arg(long)]
	pub until: Option<tg::sandbox::Status>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Timeout {
	#[arg(id = "status.timeout.timeout", long = "timeout", overrides_with = "status.timeout.no_timeout", value_parser = humantime::parse_duration)]
	pub timeout: Option<Duration>,

	#[arg(
		id = "status.timeout.no_timeout",
		long = "no-timeout",
		overrides_with = "status.timeout.timeout"
	)]
	pub no_timeout: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Get the status stream.
	let client = cli.client().await?;
	let id = args.sandbox.node.clone();
	let arg = tg::sandbox::status::Arg {
		location: args.locations.get_for_options(&args.sandbox),
		timeout: args.timeout.get(args.until.is_some()),
		tokens: args.sandbox.options.tokens,
	};
	let stream = client
		.get_sandbox_status(&id, arg)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox status"))?;

	// Print the statuses if the command does not wait for a status.
	let Some(until) = args.until else {
		cli.print_serde_stream(stream.boxed(), args.print).await?;
		return Ok(());
	};

	// Print the statuses until the sandbox has the given status or can no longer have it.
	let last = Arc::new(Mutex::new(None));
	let stream = stream
		.take_while_inclusive(move |result| {
			let done =
				matches!(result, Ok(status) if *status == until || !reachable(*status, until));
			future::ready(!done)
		})
		.inspect_ok({
			let last = last.clone();
			move |status| {
				last.lock().unwrap().replace(*status);
			}
		});
	cli.print_serde_stream(stream.boxed(), args.print).await?;
	let last = *last.lock().unwrap();

	// Fail if the stream ended while the sandbox could still have the given status, which happens when the timeout passes.
	let Some(status) = last.filter(|status| *status == until || !reachable(*status, until)) else {
		return Err(tg::error!(
			%id,
			%until,
			"the timeout passed before the sandbox had the status"
		));
	};

	// Fail unless the sandbox had the given status.
	if status != until {
		return Err(tg::error!(
			%id,
			%status,
			%until,
			"the sandbox can no longer have the status"
		));
	}

	Ok(())
}

impl Timeout {
	fn get(&self, follow: bool) -> Option<Duration> {
		if self.no_timeout {
			None
		} else if follow {
			self.timeout
		} else {
			self.timeout.or(Some(Duration::ZERO))
		}
	}
}

fn reachable(status: tg::sandbox::Status, until: tg::sandbox::Status) -> bool {
	match status {
		tg::sandbox::Status::Destroyed => until.is_destroyed(),
		tg::sandbox::Status::Destroying => until.is_destroying() || until.is_destroyed(),
		tg::sandbox::Status::Started
		| tg::sandbox::Status::Starting
		| tg::sandbox::Status::Stopped
		| tg::sandbox::Status::Stopping => true,
	}
}
