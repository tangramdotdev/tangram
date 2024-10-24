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

	/// Whether to bundle the artifact before checkout.
	#[arg(long)]
	pub bundle: bool,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(short, long, requires = "path")]
	pub force: bool,

	/// The path to check out the artifact to. The default is the artifact's ID in the checkouts directory.
	#[arg(index = 2)]
	pub path: Option<PathBuf>,

	/// Whether to check out the artifact's references.
	#[arg(long, default_value_t = true)]
	pub references: bool,
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
			bundle: path.is_some(),
			force: args.force,
			path,
			dependencies: true,
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
