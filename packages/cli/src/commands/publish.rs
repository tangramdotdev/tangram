use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Publish a package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_publish(&self, args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Create the package.
		let (package, _) = tg::package::get_with_lock(client, &args.package).await?;

		// Get the package ID.
		let id = package.id(client).await?;

		// Publish the package.
		client
			.publish_package(self.user.as_ref(), id)
			.await
			.map_err(|error| error!(source = error, "Failed to publish the package."))?;

		Ok(())
	}
}
