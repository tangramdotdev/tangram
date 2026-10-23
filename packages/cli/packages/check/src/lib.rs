use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Check.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".", index = 1)]
	pub references: Vec<tg::Reference>,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_check(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Get the modules.
		let modules = self.get_modules(&args.references).await?;
		let referents: Vec<_> = modules
			.iter()
			.map(|module| module.referent.clone().replace(()).0)
			.collect();

		// Check the modules.
		let modules = modules.iter().map(tg::Module::to_data).collect();
		let arg = tg::check::Arg {
			location: None,
			modules,
		};
		let output = client
			.check(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to check"))?;
		let success = !output
			.diagnostics
			.iter()
			.any(|diagnostic| diagnostic.severity.is_error());

		// Print the diagnostics.
		for diagnostic in output.diagnostics {
			let diagnostic: tg::Diagnostic = diagnostic.try_into()?;
			let mut diagnostic = tg::Referent::with_node(diagnostic);
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

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
