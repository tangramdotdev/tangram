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
	#[arg(long, value_parser = humantime::parse_duration, overrides_with = "no_ttl")]
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
		let arg = tg::sandbox::create::Arg {
			cpu: args.arg.cpu,
			hostname: args.arg.hostname,
			isolation: args.arg.isolation,
			location: args.location.get(),
			memory: args.arg.memory,
			mounts: args.arg.mounts,
			network: args.arg.network.get().unwrap_or(tg::Either::Left(false)),
			ttl: args.ttl.get(),
			user: args.arg.user,
		};
		let output = client
			.create_sandbox(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;
		Self::print_id(&output.id);
		Ok(())
	}
}
