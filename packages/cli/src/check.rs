use {crate::Cli, tangram_client::prelude::*};

/// Check.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".", index = 1)]
	pub references: Vec<tg::Reference>,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_check(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the modules.
		let modules = self.get_modules(&args.references).await?;
		let referents: Vec<_> = modules
			.iter()
			.map(|module| module.referent.clone().replace(()).0)
			.collect();

		// Check the modules.
		let modules = modules.iter().map(tg::Module::to_data).collect();
		let arg = tg::check::Arg {
			local: args.local.local,
			modules,
			remotes: args.remotes.remotes,
		};
		let output = handle
			.check(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check"))?;
		let success = !output
			.diagnostics
			.iter()
			.any(|diagnostic| diagnostic.severity.is_error());

		// Print the diagnostics.
		for diagnostic in output.diagnostics {
			let diagnostic: tg::Diagnostic = diagnostic.try_into()?;
			let mut diagnostic = tg::Referent::with_item(diagnostic);
			// Inherit from the first referent if available.
			if let Some(referent) = referents.first() {
				diagnostic.inherit(referent);
			}
			self.print_diagnostic(diagnostic).await;
		}

		if !success {
			return Err(tg::error!("type checking failed"));
		}

		Ok(())
	}
}
