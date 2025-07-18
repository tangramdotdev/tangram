use crate::Cli;
use std::path::PathBuf;
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;

/// Check out an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Whether to check out the artifact's dependencies.
	#[arg(long)]
	pub dependencies: Option<bool>,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(short, long, requires = "path")]
	pub force: bool,

	/// Whether to write the lock.
	#[arg(default_value = "true", long, action = clap::ArgAction::Set)]
	pub lock: bool,

	/// The path to check out the artifact to.
	#[arg(index = 2)]
	pub path: Option<PathBuf>,

	/// The artifact to check out.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_checkout(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = if let Some(path) = args.path {
			let path = std::path::absolute(path)
				.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;
			Some(path)
		} else {
			None
		};

		// Get the artifact.
		let referent = self.get_reference(&args.reference).await?;
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let artifact = tg::Artifact::try_from(object)?;
		let artifact = artifact.id();

		// Check out the artifact.
		let dependencies = args.dependencies.unwrap_or(true);
		let force = args.force;
		let lock = args.lock;
		let arg = tg::checkout::Arg {
			artifact,
			dependencies,
			force,
			lock,
			path,
		};
		let stream = handle
			.checkout(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the checkout stream"))?;
		let output = self.render_progress_stream(stream).await?;

		// Print the path.
		println!("{}", output.path.display());

		Ok(())
	}
}
