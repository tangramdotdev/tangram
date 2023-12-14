use crate::Cli;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};

/// Publish a package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_publish(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Get the user.
		let user = self.user().await?;

		// Create the package.
		let (package, _) = tg::package::get_with_lock(tg, &args.package).await?;

		// Get the package ID.
		let id = package.id(tg).await?;

		// Publish the package.
		tg.publish_package(user.as_ref(), id)
			.await
			.wrap_err("Failed to publish the package.")?;

		Ok(())
	}
}
