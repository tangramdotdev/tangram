use {crate::Cli, tangram_client::prelude::*};

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	/// Generate the documentation for the runtime.
	#[arg(long)]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_document(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

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
			local: args.local.local,
			module,
			remotes: args.remotes.remotes,
		};
		let output = handle.document(arg).await?;

		// Print the document.
		self.print_serde(output, args.print).await?;

		Ok(())
	}
}
