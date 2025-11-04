use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,

	/// Generate the documentation for the runtime.
	#[arg(long)]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_document(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

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
		let arg = tg::document::Arg { module, remote };
		let output = handle.document(arg).await?;

		// Print the document.
		self.print_serde(output, args.print).await?;

		Ok(())
	}
}
