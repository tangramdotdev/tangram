use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	/// Generate the documentation for the runtime.
	#[arg(long)]
	pub runtime: bool,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_document(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Get the module.
		let module = if args.runtime {
			tg::module::Data {
				kind: tg::module::Kind::TypeScriptDeclaration,
				referent: tg::Referent::with_node(tg::module::data::Source::Path(
					"tangram.d.ts".into(),
				)),
			}
		} else {
			self.get_module(&args.reference).await?.to_data()
		};

		// Document the module.
		let arg = tg::document::Arg {
			location: args.location.get(),
			module,
		};
		let output = client
			.document(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the document"))?;

		// Print the document.
		self.print_serde(output, args.print).await?;

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
