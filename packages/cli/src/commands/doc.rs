use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Generate documentation.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[clap(long)]
	pub locked: bool,

	#[clap(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// Generate the documentation for the runtime.
	#[clap(short, long, default_value = "false")]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_doc(&self, mut args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Get the doc.
		let doc = if args.runtime {
			client.get_runtime_doc().await?
		} else {
			client
				.try_get_package_doc(&args.package)
				.await?
				.ok_or_else(|| error!("failed to get the package"))?
		};

		// Serialize the output.
		let output = serde_json::to_string_pretty(&doc)
			.map_err(|source| error!(!source, "failed to serialize the output"))?;

		// Print the output.
		println!("{output}");

		Ok(())
	}
}
