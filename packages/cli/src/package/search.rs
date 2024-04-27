use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Search for packages.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub query: String,
}

impl Cli {
	pub async fn command_package_search(&self, args: Args) -> tg::Result<()> {
		// Perform the search.
		let arg = tg::package::list::Arg { query: args.query };
		let packages = self.handle.list_packages(arg).await?;

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
