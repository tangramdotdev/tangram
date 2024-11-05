use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;

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
	pub async fn command_package_document(&self, args: Args) -> tg::Result<()> {
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
		let referent = self.get_reference(&args.reference).await?;
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let object = if let Some(subpath) = &referent.subpath {
			let directory = object
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			directory.get(&handle, subpath).await?.into()
		} else {
			object
		};
		let Ok(package) = tg::Directory::try_from(object) else {
			return Err(tg::error!("expected a directory"));
		};
		let package = package.id(&handle).await?;

		// Document the module.
		let arg = tg::package::document::Arg { package, remote };
		let output = handle.document_package(arg).await?;

		// Serialize the output.
		let output = serde_json::to_string_pretty(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;

		// Print the output.
		println!("{output}");

		Ok(())
	}
}
