use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Search for packages.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub query: String,

	#[arg(short, long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_package_search(&self, args: Args) -> tg::Result<()> {
		// List the packages.
		let arg = tg::package::list::Arg {
			query: Some(args.query),
			remote: args.remote,
		};
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
