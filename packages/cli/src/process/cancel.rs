use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_process_cancel(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Cancel the process.
		let arg = tg::process::finish::Arg {
			error: Some(tg::error!(
				code = tg::error::Code::Cancelation,
				"the process was explicitly canceled"
			)),
			exit: None,
			output: None,
			remote,
			status: tg::process::Status::Finished,
		};
		handle.try_finish_process(&args.process, arg).await?;

		Ok(())
	}
}
