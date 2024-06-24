use crate::Cli;
use tangram_client as tg;

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// Generate the documentation for the runtime.
	#[arg(long, default_value = "false")]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_package_doc(&self, mut args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Handle the runtime doc.
		if args.runtime {
			let doc = client.get_js_runtime_doc().await?;
			let output = serde_json::to_string_pretty(&doc)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
			println!("{output}");
		}

		// If the reference has a path, then canonicalize it.
		if let tg::reference::Path::Path(path) = &mut args.reference.path {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::package::create::Arg {
			reference: args.reference,
			locked: args.locked,
			remote: remote.clone(),
		};
		let tg::package::create::Output { package } = client.create_package(arg).await?;

		// Get the doc.
		let arg = tg::package::doc::Arg { remote };
		let doc = client.document_package(&package, arg).await?;

		// Serialize the output.
		let output = serde_json::to_string_pretty(&doc)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;

		// Print the output.
		println!("{output}");

		Ok(())
	}
}
