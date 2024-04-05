use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;

/// Check out an artifact.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// The ID of the artifact to check out.
	pub id: tg::artifact::Id,

	/// The path to check out the artifact to. The default is the artifact's ID in the checkouts directory.
	pub path: Option<PathBuf>,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(short, long, requires = "path")]
	pub force: bool,
}

impl Cli {
	pub async fn command_checkout(&self, args: Args) -> tg::Result<()> {
		let client = &self.client().await?;

		// Get the artifact.
		let artifact = tg::Artifact::with_id(args.id.clone());

		// Get the path.
		let path = if let Some(path) = args.path {
			let current = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
			let path = current.join(&path);
			let parent = path
				.parent()
				.ok_or_else(|| tg::error!("the path must have a parent directory"))?;
			let file_name = path
				.file_name()
				.ok_or_else(|| tg::error!("the path must have a file name"))?;
			tokio::fs::create_dir_all(parent)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the parent directory"))?;
			let path = parent
				.canonicalize()
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.join(file_name);
			let path = path.try_into()?;
			Some(path)
		} else {
			None
		};

		// Create the arg.
		let arg = tg::artifact::CheckOutArg {
			path,
			force: args.force,
		};

		// Check out the artifact.
		let output = artifact
			.check_out(client, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

		// Print the path.
		println!("{}", output.path);

		Ok(())
	}
}
