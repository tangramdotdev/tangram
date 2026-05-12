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
	#[arg(long, overrides_with = "no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,

	#[arg(long, overrides_with = "ttl")]
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
		let network = match args.arg.network.get() {
			Some(tg::Either::Left(false)) if !ports.is_empty() => {
				return Err(tg::error!("ports require networking"));
			},
			Some(tg::Either::Right(tg::sandbox::Network::Host)) if !ports.is_empty() => {
				return Err(tg::error!("ports are not supported with host networking"));
			},
			Some(network) => network,
			None if !ports.is_empty() => tg::Either::Left(true),
			None => tg::Either::Left(false),
		};
		let arg = tg::sandbox::create::Arg {
			cpu: args.arg.cpu,
			hostname: args.arg.hostname,
			isolation: args.arg.isolation,
			location: args.location.get(),
			memory: args.arg.memory,
			mounts: args.arg.mounts,
			network,
			ports,
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
