use {crate::Cli, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(action = clap::ArgAction::Append, long = "mount", num_args = 1, short)]
	pub mounts: Vec<tg::process::Mount>,

	#[command(flatten)]
	pub network: super::Network,

	#[arg(long)]
	pub user: Option<String>,
}

impl Cli {
	pub async fn command_sandbox_create(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::sandbox::create::Arg {
			hostname: args.hostname,
			mounts: args.mounts,
			network: args.network.get(),
			user: args.user,
		};
		let output = handle
			.create_sandbox(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;
		Self::print_id(&output.id);
		Ok(())
	}
}
