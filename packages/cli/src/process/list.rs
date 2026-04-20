use {crate::Cli, tangram_client::prelude::*};

/// List processes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_process_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::list::Arg {
			location: args.locations.get(),
		};
		let output = handle
			.list_processes(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the processes"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
