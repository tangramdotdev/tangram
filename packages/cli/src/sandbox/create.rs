use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub arg: super::Options,

	#[arg(id = "create.host", long = "host")]
	pub host: Option<String>,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

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

impl Ttl {
	fn get(&self, default: Duration) -> Option<Duration> {
		if self.no_ttl {
			None
		} else {
			Some(self.ttl.unwrap_or(default))
		}
	}
}

impl Cli {
	pub async fn command_sandbox_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let cpu = crate::sandbox::cpu(&args.arg);
		let ports = args.arg.ports;
		let network = crate::sandbox::normalize_network(&args.arg.network, ports)?;
		let owner = self.resolve_owner(&client, &args.arg.owner).await?;
		let ttl = args.ttl.get(client.arg().sandbox.ttl);
		let sandbox = tg::Sandbox::builder()
			.cpu(cpu)
			.host(args.host)
			.hostname(args.arg.hostname)
			.location(args.location.get())
			.memory(crate::sandbox::resource(
				args.arg.memory_min,
				args.arg.memory,
			))
			.storage(crate::sandbox::resource(
				args.arg.storage_min,
				args.arg.storage,
			))
			.swap(crate::sandbox::resource(args.arg.swap_min, args.arg.swap))
			.mounts(args.arg.mounts)
			.network(network)
			.owner(owner)
			.ttl(ttl)
			.build_with_instance(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the sandbox"))?;
		Self::print_referent(&sandbox.to_referent(), &args.print);
		Ok(())
	}
}
