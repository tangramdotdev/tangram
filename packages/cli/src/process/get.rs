use {crate::Cli, tangram_client::prelude::*};

/// Get a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	/// The remote to get the process from.
	#[arg(long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_process_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::get::Arg {
			remote: args.remote,
		};
		let output = handle
			.try_get_process(&args.process, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
