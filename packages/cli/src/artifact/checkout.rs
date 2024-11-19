use crate::Cli;
use std::path::PathBuf;
use tangram_client::{self as tg, Handle as _};

/// Check out an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The artifact to check out.
	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(short, long, requires = "path")]
	pub force: bool,

	/// The path to check out the artifact to.
	#[arg(index = 2)]
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_artifact_checkout(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = if let Some(path) = args.path {
			let path = std::path::absolute(path)
				.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;
			Some(path)
		} else {
			None
		};

		// Check out the artifact.
		let arg = tg::artifact::checkout::Arg {
			force: args.force,
			path,
		};
		let stream = handle
			.check_out_artifact(&args.artifact, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create check out stream"))?;
		let output = self.render_progress_stream(stream).await?;

		// Print the path.
		println!("{}", output.path.display());

		Ok(())
	}
}
