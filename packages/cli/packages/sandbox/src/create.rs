use {std::time::Duration, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub arg: super::Options,

	#[usage(long = "host")]
	pub host: Option<String>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[usage(flatten)]
	pub time_to_live: TimeToLive,

	#[usage(flatten)]
	pub time_to_stop: TimeToStop,

	/// Wait for the sandbox to start.
	#[usage(long = "wait")]
	pub wait: bool,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct TimeToLive {
	/// Keep the sandbox until it is destroyed.
	#[usage(
		alias = "no-ttl",
		long = "no-time-to-live",
		overrides("--time-to-live")
	)]
	pub no_time_to_live: bool,

	/// The time that the sandbox may go unused before it is destroyed.
	#[usage(alias = "ttl", long = "time-to-live", overrides("--no-time-to-live"))]
	pub time_to_live: Option<humantime::Duration>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct TimeToStop {
	/// Keep the sandbox running until it is stopped.
	#[usage(long = "no-time-to-stop", overrides("--time-to-stop"))]
	pub no_time_to_stop: bool,

	/// The time that the sandbox may run without processes before it is stopped.
	#[usage(long = "time-to-stop", overrides("--no-time-to-stop"))]
	pub time_to_stop: Option<humantime::Duration>,
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
			self.time_to_live.map(Into::into).or(default)
		}
	}
}

impl TimeToStop {
	fn get(&self, default: Option<Duration>) -> Option<Duration> {
		if self.no_time_to_stop {
			None
		} else {
			self.time_to_stop.map(Into::into).or(default)
		}
	}
}
