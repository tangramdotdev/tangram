use crate::Cli;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};

/// Generate documentation.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// Generate the documentation for the runtime.
	#[arg(short, long, default_value = "false")]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_doc(&self, mut args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.wrap_err("Failed to canonicalize the path.")?
				.try_into()?;
		}

		// Get the doc.
		let doc = if args.runtime {
			client.get_runtime_doc().await?
		} else {
			client
				.try_get_package_doc(&args.package)
				.await?
				.wrap_err("Failed to get the package.")?
		};

		// Serialize the output.
		let output =
			serde_json::to_string_pretty(&doc).wrap_err("Failed to serialize the output.")?;

		// Print the output.
		println!("{output}");

		Ok(())
	}
}
