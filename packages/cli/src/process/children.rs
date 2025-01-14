use crate::Cli;
use futures::TryStreamExt as _;
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};

/// Get a build's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub length: Option<u64>,

	#[arg(long)]
	pub position: Option<u64>,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	#[arg(long)]
	pub size: Option<u64>,
}

impl Cli {
	pub async fn command_process_children(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the children.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::process::children::get::Arg {
			position: args.position.map(std::io::SeekFrom::Start),
			length: args.length,
			remote,
			size: args.size,
		};
		let stream = handle.get_process_children(&args.process, arg).await?;

		// Print the children.
		let mut stream = pin!(stream);
		while let Some(chunk) = stream.try_next().await? {
			for child in chunk.data {
				println!("{child}");
			}
		}

		Ok(())
	}
}
