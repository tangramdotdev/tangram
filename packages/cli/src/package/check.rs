use crate::Cli;
use tangram_client as tg;

/// Check a package.
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
}

impl Cli {
	pub async fn command_package_check(&self, mut args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// If the reference has a path, then canonicalize it.
		if let tg::reference::Path::Path(path) = &mut args.reference.path {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the lock.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::package::create::Arg {
			reference: args.reference,
			locked: args.locked,
			remote: remote.clone(),
		};
		let tg::package::create::Output { package } = client.create_package(arg).await?;

		// Check the package.
		let arg = tg::package::check::Arg { remote };
		let output = client.check_package(&package, arg).await?;

		// Print the diagnostics.
		for diagnostic in &output.diagnostics {
			self.print_diagnostic(&client, diagnostic).await;
		}

		if !output.diagnostics.is_empty() {
			return Err(tg::error!("type checking failed"));
		}

		Ok(())
	}
}
