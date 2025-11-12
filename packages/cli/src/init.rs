use {crate::Cli, indoc::formatdoc, std::path::PathBuf, tangram_client::prelude::*};

/// Initialize a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_init(&mut self, args: Args) -> tg::Result<()> {
		// Get the path.
		let path = std::path::absolute(args.path.unwrap_or_default())
			.map_err(|source| tg::error!(!source, "failed to get the path"))?;

		// Create the directory.
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| tg::error!(!source, ?path, "failed to create the directory"))?;

		// Check if there is already a root module for the path.
		for name in tg::package::ROOT_MODULE_FILE_NAMES {
			let module_path = path.join(name);
			let exists = tokio::fs::try_exists(&module_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to check if the path exists"))?;
			if exists {
				return Err(
					tg::error!(module_path = %module_path.display(), "found existing root module"),
				);
			}
		}

		// Determine the reference to use for the autobuild package.
		let autobuild_reference = if let Some(autobuild_reference) = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.init_autobuild_reference.as_ref())
		{
			autobuild_reference.to_string()
		} else {
			"autobuild".to_owned()
		};

		// Define the files to generate.
		let mut files = Vec::new();
		files.push((
			path.join("tangram.ts"),
			formatdoc!(
				r#"
					import * as autobuild from "{autobuild_reference}";
					import * as std from "std";
					import source from "." with {{ type: "directory" }};
					export default () => autobuild.build({{ env: env(), source }});
					export const env = () => std.env(autobuild.env({{ source }}));
				"#,
			),
		));

		// Write the files.
		for (path, contents) in files {
			tokio::fs::write(&path, &contents).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to write the file"),
			)?;
		}

		Ok(())
	}
}
