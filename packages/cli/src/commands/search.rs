use crate::Cli;
use tangram_client as tg;
use tangram_error::Result;

/// Search for packages.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	pub query: String,
}

impl Cli {
	pub async fn command_search(&self, args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Perform the search.
		let arg = tg::package::SearchArg { query: args.query };
		let packages = client.search_packages(arg).await?;

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
