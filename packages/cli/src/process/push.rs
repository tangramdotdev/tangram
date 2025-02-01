use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Push a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub commands: bool,
}

impl Cli {
	pub async fn command_process_push(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Push the process.
		let arg = tg::process::push::Arg {
			logs: args.logs,
			outputs: true,
			recursive: args.recursive,
			remote,
			commands: args.commands,
		};
		let stream = handle.push_process(&args.process, arg).await?;
		self.render_progress_stream(stream).await?;

		Ok(())
	}
}
