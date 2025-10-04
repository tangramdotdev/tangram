use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Get process metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub pretty: Option<bool>,

	/// The remote to get the metadata from.
	#[arg(long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_process_metadata(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::metadata::Arg {
			remote: args.remote,
		};
		let metadata = handle
			.try_get_process_metadata(&args.process, arg)
			.await
			.map_err(
				|source| tg::error!(!source, %id = args.process, "failed to get the process metadata"),
			)?
			.ok_or_else(|| tg::error!("failed to get the process metadata"))?;
		Self::print_json(&metadata, args.pretty).await?;
		Ok(())
	}
}
