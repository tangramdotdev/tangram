use crate::Cli;
use tangram_client as tg;

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	#[arg(index = 2)]
	pub token: String,
}

impl Cli {
	pub async fn command_process_cancel(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Cancel the process.
		let process = tg::Process::new(args.process, None, remote, None, Some(args.token));
		process
			.cancel(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to cancel the process"))?;

		Ok(())
	}
}
