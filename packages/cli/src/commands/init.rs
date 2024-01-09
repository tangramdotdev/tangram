use crate::Cli;
use indoc::formatdoc;
use std::path::PathBuf;
use tangram_error::{error, Result, Wrap, WrapErr};

/// Initialize a new package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	/// The directory to initialize the package in.
	pub path: Option<PathBuf>,
	
	/// The name of the package. Defaults to the directory name.
	#[arg(long)]
	pub name: Option<String>,
	
	/// The version of the package. Defaults to "0.0.0".
	#[arg(long, default_value = "0.0.0")]
	pub version: String,
}

impl Cli {
	pub async fn command_init(&self, args: Args) -> Result<()> {
		// Get the path.
		let mut path = std::env::current_dir().wrap_err("Failed to get the working directory.")?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}

		// Ensure there is a directory at the path.
		match tokio::fs::metadata(&path).await {
			Ok(metadata) => {
				if !metadata.is_dir() {
					return Err(error!("The path must be a directory."));
				}
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				tokio::fs::create_dir_all(&path).await.wrap_err_with(|| {
					let path = path.display();
					format!(r#"Failed to create the directory at "{path}"."#)
				})?;
			},
			Err(error) => return Err(error.wrap("Failed to get the metadata for the path.")),
		};

		// Get the name and version.
		let name = if let Some(name) = args.name {
			name
		} else {
			path.file_name()
				.ok_or_else(|| error!("The path must have a directory name."))?
				.to_str()
				.unwrap()
				.to_owned()
		};
		let version = args.version;

		// Define the files to generate.
		let mut files = Vec::new();
		files.push((
			path.join("tangram.ts"),
			formatdoc!(
				r#"
					export let metadata = {{
						name: "{name}",
						version: "{version}",
					}};

					tg.target("default", () => "Hello, World!");
				"#,
			),
		));

		// Write the files.
		for (path, contents) in files {
			tokio::fs::write(&path, &contents)
				.await
				.wrap_err("Failed to write the file.")?;
		}

		Ok(())
	}
}
