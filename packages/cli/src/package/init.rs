use crate::Cli;
use indoc::formatdoc;
use std::path::PathBuf;
use tangram_client as tg;

/// Initialize a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_package_init(&self, args: Args) -> tg::Result<()> {
		// Get the path.
		let path = std::path::absolute(args.path.unwrap_or_default())
			.map_err(|source| tg::error!(!source, "failed to get the path"))?;

		// Create the directory.
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| tg::error!(!source, ?path, "failed to create the directory"))?;

		// Define the files to generate.
		let mut files = Vec::new();
		files.push((
			path.join("tangram.ts"),
			formatdoc!(
				r#"
					export default tg.target(() => tg.file("Hello, World!"));
				"#,
			),
		));

		// Write the files.
		for (path, contents) in files {
			tokio::fs::write(&path, &contents).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to write the file"),
			)?;
		}

		Ok(())
	}
}
