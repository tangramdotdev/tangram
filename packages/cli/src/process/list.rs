use {crate::Cli, tangram_client::prelude::*};

/// List processes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_process_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::list::Arg::default();
		let output = handle
			.list_processes(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the processes"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
