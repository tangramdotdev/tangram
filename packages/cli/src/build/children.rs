use crate::Cli;
use futures::{StreamExt as _, TryStreamExt as _};
use tangram_client::{self as tg, Handle as _};

/// Get a build's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,

	#[arg(long)]
	pub length: Option<u64>,

	#[arg(long)]
	pub position: Option<u64>,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub size: Option<u64>,
}

impl Cli {
	pub async fn command_build_children(&self, args: Args) -> tg::Result<()> {
		// Get the children.
		let arg = tg::build::children::get::Arg {
			position: args.position.map(std::io::SeekFrom::Start),
			length: args.length,
			remote: args.remote,
			size: args.size,
		};
		let mut stream = self
			.handle
			.get_build_children(&args.build, arg)
			.await?
			.boxed();

		// Print the children.
		while let Some(chunk) = stream.try_next().await? {
			for child in chunk.data {
				println!("{child}");
			}
		}

		Ok(())
	}
}
