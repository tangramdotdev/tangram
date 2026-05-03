use {crate::Cli, tangram_client::prelude::*};

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	/// Generate the documentation for the runtime.
	#[arg(long)]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_document(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Get the module.
		let module = if args.runtime {
			tg::module::Data {
				kind: tg::module::Kind::Dts,
				referent: tg::Referent::with_item(tg::module::data::Item::Path(
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
			.map_err(|source| tg::error!(!source, "failed to get the document"))?;

		// Print the document.
		self.print_serde(output, args.print).await?;

		Ok(())
	}
}
