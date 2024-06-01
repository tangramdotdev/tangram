use crate::Cli;
use futures::{StreamExt, TryStreamExt as _};
use tangram_client::{self as tg, Handle as _};

/// Get a build's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub length: Option<u64>,

	#[arg(long)]
	pub position: Option<u64>,

	#[arg(long)]
	pub size: Option<u64>,

	#[arg(long)]
	pub timeout: Option<f64>,

	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_children(&self, args: Args) -> tg::Result<()> {
		// Get the children.
		let arg = tg::build::children::Arg {
			position: args.position.map(std::io::SeekFrom::Start),
			length: args.length,
			size: args.size,
			timeout: args.timeout.map(std::time::Duration::from_secs_f64),
		};
		let mut stream = self
			.handle
			.get_build_children(&args.build, arg)
			.await?
			.boxed();

		// Print the children.
		while let Some(chunk) = stream.try_next().await? {
			for child in chunk.items {
				println!("{child}");
			}
		}

		Ok(())
	}
}
