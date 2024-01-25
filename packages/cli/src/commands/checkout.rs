use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};

/// Check out an artifact.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	/// The ID of the artifact to check out.
	pub id: tg::artifact::Id,

	/// The path to check out the artifact to.
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_checkout(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Get the path.
		let mut path = std::env::current_dir().wrap_err("Failed to get the working directory.")?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		} else {
			path.push(args.id.to_string());
		};

		// Check out the artifact.
		let artifact = tg::Artifact::with_id(args.id.clone());
		let path = if let Some(path) = &args.path {
			Some(path.clone().try_into()?)
		} else {
			None
		};
		artifact
			.check_out(tg, path.as_ref())
			.await
			.wrap_err("Failed to check out the artifact.")?;

		// Print the path.
		let path = args
			.path
			.clone()
			.unwrap_or_else(|| self.path.join(format!("artifacts/{}", args.id)));
		println!("{}", path.display());

		Ok(())
	}
}
