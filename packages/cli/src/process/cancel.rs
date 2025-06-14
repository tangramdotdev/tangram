use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub force: bool,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_process_cancel(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Cancel the process.
		let arg = tg::process::finish::Arg {
			checksum: None,
			error: Some(
				tg::error!(
					code = tg::error::Code::Cancelation,
					"the process was explicitly canceled"
				)
				.to_data(),
			),
			exit: 1,
			force: args.force,
			output: None,
			remote,
		};
		handle.finish_process(&args.process, arg).await?;

		Ok(())
	}
}
