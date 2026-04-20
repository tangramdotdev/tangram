use {crate::Cli, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub arg: super::Options,

	#[command(flatten)]
	pub location: crate::location::Args,
}

impl Cli {
	pub async fn command_sandbox_create(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::sandbox::create::Arg {
			cpu: args.arg.cpu,
			hostname: args.arg.hostname,
			location: args.location.get(),
			memory: args.arg.memory,
			mounts: args.arg.mounts,
			network: args.arg.network.get(),
			ttl: args.arg.ttl.unwrap_or(i64::MAX as u64),
			user: args.arg.user,
		};
		let output = handle
			.create_sandbox(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;
		Self::print_id(&output.id);
		Ok(())
	}
}
