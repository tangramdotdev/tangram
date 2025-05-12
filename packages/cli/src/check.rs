use crate::Cli;
use tangram_client::{self as tg, prelude::*};

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
	pub async fn command_check(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the module.
		let module = self.get_module(&args.reference).await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Check the module.
		let module = module.to_data();
		let arg = tg::check::Arg { module, remote };
		let output = handle.check(arg).await?;

		// Print the diagnostics.
		for diagnostic in &output.diagnostics {
			Self::print_diagnostic(diagnostic);
		}

		if !output.diagnostics.is_empty() {
			return Err(tg::error!("type checking failed"));
		}

		Ok(())
	}
}
