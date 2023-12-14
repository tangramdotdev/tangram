use super::PackageArgs;
use crate::Cli;
use tangram_client as tg;
use tangram_error::{return_error, Result};

/// Check a package for errors.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	#[command(flatten)]
	pub package_args: PackageArgs,
}

impl Cli {
	pub async fn command_check(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Create the language server.
		let server = tangram_language::Server::new(tg, tokio::runtime::Handle::current());

		// Get the package.
		let (package, lock) = tg::package::get_with_lock(tg, &args.package).await?;

		// Check the package for diagnostics.
		let diagnostics = server
			.check(vec![tangram_language::Module::Normal(
				tangram_language::module::Normal {
					package: package.id(tg).await?.clone(),
					lock: lock.id(tg).await?.clone(),
					path: tg::package::ROOT_MODULE_FILE_NAME.parse().unwrap(),
				},
			)])
			.await?;

		// Print the diagnostics.
		for diagnostic in &diagnostics {
			// Get the diagnostic location and message.
			let tangram_language::Diagnostic {
				location, message, ..
			} = diagnostic;

			// Print the location if one is available.
			if let Some(location) = location {
				println!("{location}");
			}

			// Print the diagnostic message.
			println!("{message}");

			// Print a newline.
			println!();
		}

		if !diagnostics.is_empty() {
			return_error!("Type checking failed.");
		}

		Ok(())
	}
}
