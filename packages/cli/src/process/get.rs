use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Get a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub pretty: Option<bool>,

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
		let tg::process::get::Output { data, .. } = handle
			.try_get_process(&args.process, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))?;
		Self::print_json(&data, args.pretty).await?;
		Ok(())
	}
}
