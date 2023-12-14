use crate::Cli;
use tangram_error::Result;

/// Search for packages.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	pub query: String,
}

impl Cli {
	pub async fn command_search(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Perform the search.
		let packages = tg.search_packages(&args.query).await?;

		// Print the package names.
		if packages.is_empty() {
			println!("No packages matched your query.");
		}
		for package in packages {
			println!("{package}");
		}

		Ok(())
	}
}
