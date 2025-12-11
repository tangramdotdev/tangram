use {crate::Cli, tangram_client::prelude::*};

/// Check a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_check(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the module.
		let module = self.get_module(&args.reference).await?;
		let (referent, _) = module.referent.clone().replace(());

		// Check the module.
		let module = module.to_data();
		let arg = tg::check::Arg {
			local: args.local.local,
			module,
			remotes: args.remotes.remotes,
		};
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
