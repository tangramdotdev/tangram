use crate::Cli;
use either::Either;
use tangram_client::{self as tg, Handle as _};

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(index = 1, default_value = ".?kind=package")]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// Generate the documentation for the runtime.
	#[arg(long, default_value = "false")]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_package_doc(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Handle the runtime doc.
		if args.runtime {
			let doc = handle.get_js_runtime_doc().await?;
			let output = serde_json::to_string_pretty(&doc)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
			println!("{output}");
		}

		// Get the reference.
		let item = self.get_reference(&args.reference).await?;

		// Get the file.
		let Either::Right(tg::Object::File(file)) = item else {
			return Err(tg::error!("expected a file"));
		};

		// Document the file.
		let file = file.id(&handle).await?;
		let arg = tg::artifact::document::Arg {
			artifact: file.into(),
			remote,
		};
		let doc = handle.document_artifact(arg).await?;

		// Serialize the output.
		let output = serde_json::to_string_pretty(&doc)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;

		// Print the output.
		println!("{output}");

		Ok(())
	}
}