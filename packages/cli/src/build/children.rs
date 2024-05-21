use crate::Cli;
use futures::StreamExt;
use tangram_client::{self as tg, Handle as _};

/// List a build's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub position: Option<u64>,

	#[arg(short, long)]
	pub length: Option<u64>,

	#[arg(short, long)]
	pub size: Option<u64>,

	#[arg(short, long)]
	pub timeout: Option<f64>,

	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_children(&self, args: Args) -> tg::Result<()> {
		let arg = tg::build::children::Arg {
			position: args.position.map(std::io::SeekFrom::Start),
			length: args.length,
			size: args.size,
			timeout: args.timeout.map(std::time::Duration::from_secs_f64),
		};
		let mut stream = self
			.handle
			.try_get_build_children(&args.build, arg)
			.await?
			.ok_or_else(|| tg::error!(%id = args.build, "expected the build to exist"))?
			.boxed();

		while let Some(chunk) = stream.next().await {
			let chunk = chunk.map_err(|source| tg::error!(!source, "expected a chunk"))?;
			for item in chunk.items {
				println!("{item}");
			}
		}

		Ok(())
	}
}
