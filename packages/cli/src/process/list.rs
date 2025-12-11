use {crate::Cli, tangram_client::prelude::*};

/// List processes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_process_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::list::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		let output = handle
			.list_processes(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the processes"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
