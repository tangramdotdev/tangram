use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

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
	pub async fn command_package_doc(&self, mut args: Args) -> tg::Result<()> {
		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Get the doc.
		let doc = if args.runtime {
			self.handle.get_js_runtime_doc().await?
		} else {
			self.handle
				.try_get_package_doc(&args.package)
				.await?
				.ok_or_else(|| tg::error!("failed to get the package"))?
		};

		// Serialize the output.
		let output = serde_json::to_string_pretty(&doc)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;

		// Print the output.
		println!("{output}");

		Ok(())
	}
}
