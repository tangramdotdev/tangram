use {
	futures::{StreamExt as _, TryStreamExt as _, future},
	std::{
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
	tangram_futures::stream::Ext as _,
	usage_rs as usage,
};

/// Get a process's status.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	pub reference: tg::Reference,

	#[usage(flatten)]
	pub timeout: Timeout,

	/// Wait until the process's status is this status, and fail if the process can no longer have it.
	#[usage(long)]
	pub until: Option<tg::process::Status>,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Timeout {
	#[usage(long = "timeout", overrides("--no-timeout"))]
	pub timeout: Option<humantime::Duration>,

	#[usage(long = "no-timeout", overrides("--timeout"))]
	pub no_timeout: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Get the status stream.
	let client = cli.client().await?;
	let mut locations = args.locations;
	locations.set_from_reference_if_unset(&args.reference);
	let process =
		tangram_cli_reference::get_process_with_locations(cli, &args.reference, &locations).await?;
	let id = process.node.clone();
	let location = locations.get_for_options(&process);
	let process = tg::Process::<tg::Value>::with_referent(process);
	let options = tg::process::status::Options {
		location: location.clone(),
		timeout: args.timeout.get(args.until.is_some()),
	};
	let stream = process
		.status_with_instance(&client, options)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to get the process status"))?;

	// Print the statuses if the command does not wait for a status.
	let Some(until) = args.until else {
		cli.print_serde_stream(stream.boxed(), args.print).await?;
		return Ok(());
	};

	// Print the statuses until the process has the given status or can no longer have it.
	let last = Arc::new(Mutex::new(None));
	let stream = stream
		.take_while_inclusive(move |result| {
			let done = matches!(result, Ok(status) if order(*status) >= order(until));
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

	// Fail if the stream ended before the process had the given status or a later one, which happens when the timeout passes.
	let Some(status) = last.filter(|status| order(*status) >= order(until)) else {
		return Err(tg::error!(
			%id,
			%until,
			"the timeout passed before the process had the status"
		));
	};

	// Fail unless the process had the given status.
	match status {
		status if status == until => (),
		tg::process::Status::Finished if until.is_started() => {
			let options = tg::process::get::Options {
				location,
				..Default::default()
			};
			let output = process
				.get_with_instance(&client, options)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
			if output.data.started_at.is_none() {
				return Err(tg::error!(%id, "the process finished without starting"));
			}
		},
		status => {
			return Err(tg::error!(
				%id,
				%status,
				%until,
				"the process can no longer have the status"
			));
		},
	}

	Ok(())
}

impl Timeout {
	fn get(&self, follow: bool) -> Option<Duration> {
		if self.no_timeout {
			None
		} else if follow {
			self.timeout.map(Into::into)
		} else {
			self.timeout.map(Into::into).or(Some(Duration::ZERO))
		}
	}
}

fn order(status: tg::process::Status) -> u8 {
	match status {
		tg::process::Status::Finished => 2,
		tg::process::Status::Queued => 0,
		tg::process::Status::Started => 1,
	}
}
