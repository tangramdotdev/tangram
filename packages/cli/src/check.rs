use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Check a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_check(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the module.
		let module = self.get_module(&args.reference).await?;
		let (referent, _) = module.referent.clone().replace(());

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Check the module.
		let module = module.to_data();
		let arg = tg::check::Arg { module, remote };
		let output = handle.check(arg).await?;
		let success = !output
			.diagnostics
			.iter()
			.any(|diagnostic| diagnostic.severity.is_error());

		// Print the diagnostics.
		for diagnostic in output.diagnostics {
			let diagnostic: tg::Diagnostic = diagnostic.try_into()?;
			let mut diagnostic = tg::Referent::with_item(diagnostic);
			diagnostic.inherit(&referent);
			self.print_diagnostic(diagnostic).await;
		}

		if !success {
			return Err(tg::error!("type checking failed"));
		}

		Ok(())
	}
}
