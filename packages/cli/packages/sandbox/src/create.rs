use {std::time::Duration, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub arg: super::Options,

	#[arg(id = "create.host", long = "host")]
	pub host: Option<String>,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[command(flatten)]
	pub time_to_live: TimeToLive,

	#[command(flatten)]
	pub time_to_stop: TimeToStop,

	/// Wait for the sandbox to start.
	#[arg(id = "create.wait", long = "wait")]
	pub wait: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct TimeToLive {
	/// Keep the sandbox until it is destroyed.
	#[arg(
		alias = "no-ttl",
		id = "create.time_to_live.no_time_to_live",
		long = "no-time-to-live",
		overrides_with = "create.time_to_live.time_to_live"
	)]
	pub no_time_to_live: bool,

	/// The time that the sandbox may go unused before it is destroyed.
	#[arg(alias = "ttl", id = "create.time_to_live.time_to_live", long = "time-to-live", overrides_with = "create.time_to_live.no_time_to_live", value_parser = humantime::parse_duration)]
	pub time_to_live: Option<Duration>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct TimeToStop {
	/// Keep the sandbox running until it is stopped.
	#[arg(
		id = "create.time_to_stop.no_time_to_stop",
		long = "no-time-to-stop",
		overrides_with = "create.time_to_stop.time_to_stop"
	)]
	pub no_time_to_stop: bool,

	/// The time that the sandbox may run without processes before it is stopped.
	#[arg(id = "create.time_to_stop.time_to_stop", long = "time-to-stop", overrides_with = "create.time_to_stop.no_time_to_stop", value_parser = humantime::parse_duration)]
	pub time_to_stop: Option<Duration>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Resolve the sandbox settings.
	let client = cli.client().await?;
	let cpu = crate::cpu(&args.arg);
	let ports = args.arg.ports;
	let network = crate::normalize_network(&args.arg.network, ports)?;
	let owner = crate::resolve_owner(&client, &args.arg.owner).await?;
	let time_to_live = args.time_to_live.get(client.arg().sandbox.time_to_live);
	let time_to_stop = args.time_to_stop.get(client.arg().sandbox.time_to_stop);

	// Create the sandbox.
	let sandbox = tg::Sandbox::builder()
		.cpu(cpu)
		.host(args.host)
		.hostname(args.arg.hostname)
		.location(args.location.get())
		.memory(crate::resource(args.arg.memory_min, args.arg.memory))
		.storage(crate::resource(args.arg.storage_min, args.arg.storage))
		.mounts(args.arg.mounts)
		.network(network)
		.owner(owner)
		.time_to_live(time_to_live)
		.time_to_stop(time_to_stop)
		.wait(args.wait)
		.build_with_instance(&client)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the sandbox"))?;

	// Print the sandbox referent.
	Cli::print_referent(&sandbox.to_referent(), &args.print);

	Ok(())
}

impl TimeToLive {
	fn get(&self, default: Option<Duration>) -> Option<Duration> {
		if self.no_time_to_live {
			None
		} else {
			self.time_to_live.or(default)
		}
	}
}

impl TimeToStop {
	fn get(&self, default: Option<Duration>) -> Option<Duration> {
		if self.no_time_to_stop {
			None
		} else {
			self.time_to_stop.or(default)
		}
	}
}
