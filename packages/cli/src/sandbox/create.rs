use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub arg: super::Options,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub ttl: Ttl,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ttl {
	#[arg(id = "create.ttl.ttl", long = "ttl", overrides_with = "create.ttl.no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,

	#[arg(
		id = "create.ttl.no_ttl",
		long = "no-ttl",
		overrides_with = "create.ttl.ttl"
	)]
	pub no_ttl: bool,
}

impl Ttl {
	fn get(&self) -> Option<Duration> {
		if self.no_ttl { None } else { self.ttl }
	}
}

impl Cli {
	pub async fn command_sandbox_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let ports = args.arg.ports;
		let network = crate::sandbox::normalize_network(&args.arg.network, ports)?;
		let owner = self.resolve_owner(&client, args.arg.owner).await?;
		let arg = tg::sandbox::create::Arg {
			cpu: args.arg.cpu,
			hostname: args.arg.hostname,
			isolation: args.arg.isolation,
			location: args.location.get(),
			memory: args.arg.memory,
			mounts: args.arg.mounts,
			network,
			owner,
			ttl: args.ttl.get(),
			user: args.arg.user,
		};
		let output = client
			.create_sandbox(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the sandbox"))?;
		Self::print_id(&output.id);
		Ok(())
	}
}
