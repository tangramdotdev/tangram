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
	pub ttl: Ttl,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ttl {
	#[arg(
		id = "create.ttl.no_ttl",
		long = "no-ttl",
		overrides_with = "create.ttl.ttl"
	)]
	pub no_ttl: bool,

	#[arg(id = "create.ttl.ttl", long = "ttl", overrides_with = "create.ttl.no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Resolve the sandbox settings.
	let client = cli.client().await?;
	let cpu = crate::cpu(&args.arg);
	let ports = args.arg.ports;
	let network = crate::normalize_network(&args.arg.network, ports)?;
	let owner = crate::resolve_owner(&client, &args.arg.owner).await?;
	let ttl = args.ttl.get(client.arg().sandbox.ttl);

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
		.ttl(ttl)
		.build_with_instance(&client)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the sandbox"))?;

	// Print the sandbox referent.
	Cli::print_referent(&sandbox.to_referent(), &args.print);

	Ok(())
}

impl Ttl {
	fn get(&self, default: Duration) -> Option<Duration> {
		if self.no_ttl {
			None
		} else {
			Some(self.ttl.unwrap_or(default))
		}
	}
}
