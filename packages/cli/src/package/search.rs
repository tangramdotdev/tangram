use crate::Cli;
use tangram_client as tg;

/// Search for packages.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub query: String,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_package_search(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// List the packages.
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::package::list::Arg {
			query: Some(args.query),
			remote,
		};
		let packages = client.list_packages(arg).await?;

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
